#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Final


REPO: Final = Path(__file__).resolve().parents[1]
SCRIPT: Final = REPO / "tools/compaction_io_experiment_summary.py"
DEPTHS: Final = (2, 4, 8, 16, 32)


class SummaryGeneratorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name) / "runs"
        self.summary = Path(self.temp_dir.name) / "summary"
        self.root.mkdir()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def make_run(self, root: Path, run_id: str, depth: int, repetition: int) -> None:
        run_dir = root / run_id
        run_dir.mkdir()
        histogram = {
            "count": 1,
            "avg": 2.0,
            "stddev": 0.0,
            "p50": 2.0,
            "p95": 2.0,
            "p99": 2.0,
            "max": 2.0,
        }
        metrics = {
            "compaction_io_experiment": {
                "depth": depth,
                "io_service_us": histogram,
                "sync_fallback_read_us": {
                    "count": 1,
                    "avg": 100.0,
                    "stddev": 0.0,
                    "p50": 100.0,
                    "p95": 100.0,
                    "p99": 100.0,
                    "max": 100.0,
                },
            }
        }
        interval = {
            "depth": depth,
            "t_compaction_us": 100.0,
            "cpu_time_us": 50.0,
            "t_exposed_io_us": 5.0,
            "overlap_total_us": 3.0,
            "stall_count": 1,
            "outstanding_async_reads": 0,
            "request_span_bytes": 1048576,
            "physical_request_count": 1,
            "physical_read_bytes": 1024.0,
            "prefetched_bytes": 1024.0,
            "logical_input_bytes": 1024.0,
            "output_bytes": 512.0,
        }
        iostat_row = "{} 0 1024 0 0 1 0 0 2048 0 0 2 0 0 0 0 0 0 0 0 0 3 4\n"
        (run_dir / "metrics.json").write_text(json.dumps(metrics))
        (run_dir / "interval.json").write_text(json.dumps(interval))
        (run_dir / "status.txt").write_text(
            f"run_id={run_id}\ndepth={depth}\nrepetition={repetition}\n"
            "benchmark_status=0\niostat_status=143\nclassification=accepted\n"
        )
        (run_dir / "input-sst-before-device.txt").write_text("66306 input\n")
        (run_dir / "output-sst-after-device.txt").write_text("66309 output\n")
        (run_dir / "iostat.raw").write_text(
            iostat_row.format("nvme0n1") + iostat_row.format("nvme1n1")
        )

    def make_batch(self, root: Path, batch_id: str) -> None:
        for depth in DEPTHS:
            for repetition in (1, 2, 3):
                run_id = f"{batch_id}-d{depth:02d}-r{repetition}"
                self.make_run(root, run_id, depth, repetition)

    def run_generator(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), *arguments],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_fresh_schema_batch_excludes_stale_uring_directories(self) -> None:
        batch_id = "batch-20260831T015921Z"
        self.make_batch(self.root, batch_id)
        self.make_run(self.root, "uring_d02_r1", 2, 1)

        result = self.run_generator(
            "--batch",
            batch_id,
            "--runs-root",
            str(self.root),
            "--summary-dir",
            str(self.summary),
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        rows = (self.summary / "per-run.csv").read_text().splitlines()
        self.assertEqual(len(rows), 16)
        self.assertTrue(all("uring_" not in row for row in rows))
        self.assertIn("Accepted runs: 15", (self.summary.parent / "report.md").read_text())

    def test_missing_sync_fallback_field_rejects_run_and_batch(self) -> None:
        batch_id = "batch-20260831T015921Z"
        run_id = f"{batch_id}-d02-r1"
        self.make_batch(self.root, batch_id)
        metrics_path = self.root / run_id / "metrics.json"
        metrics = json.loads(metrics_path.read_text())
        del metrics["compaction_io_experiment"]["sync_fallback_read_us"]
        metrics_path.write_text(json.dumps(metrics))

        result = self.run_generator(
            "--batch",
            batch_id,
            "--runs-root",
            str(self.root),
            "--summary-dir",
            str(self.summary),
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(batch_id, result.stderr)
        self.assertIn(run_id, result.stderr)
        self.assertIn("sync_fallback_read_us", result.stderr)
        self.assertFalse(self.summary.exists())

    def test_old_schema_batch_is_rejected(self) -> None:
        batch_id = "batch-20260831T015921Z"
        self.make_batch(self.root, batch_id)
        for run_dir in self.root.iterdir():
            metrics_path = run_dir / "metrics.json"
            metrics = json.loads(metrics_path.read_text())
            del metrics["compaction_io_experiment"]["sync_fallback_read_us"]
            metrics_path.write_text(json.dumps(metrics))

        result = self.run_generator(
            "--batch",
            batch_id,
            "--runs-root",
            str(self.root),
            "--summary-dir",
            str(self.summary),
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(batch_id, result.stderr)
        self.assertIn("sync_fallback_read_us", result.stderr)

    def test_async_histogram_uses_async_samples_only(self) -> None:
        batch_id = "batch-20260831T015921Z"
        run_id = f"{batch_id}-d02-r1"
        self.make_batch(self.root, batch_id)
        metrics_path = self.root / run_id / "metrics.json"
        metrics = json.loads(metrics_path.read_text())
        async_histogram = metrics["compaction_io_experiment"]["io_service_us"]
        async_histogram["count"] = 2
        async_histogram["avg"] = 7.0
        sync_histogram = metrics["compaction_io_experiment"]["sync_fallback_read_us"]
        sync_histogram["count"] = 0
        sync_histogram["avg"] = 0.0
        metrics_path.write_text(json.dumps(metrics))

        result = self.run_generator(
            "--batch",
            batch_id,
            "--runs-root",
            str(self.root),
            "--summary-dir",
            str(self.summary),
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        with (self.summary / "per-run.csv").open() as stream:
            rows = list(csv.DictReader(stream))
        row = next(item for item in rows if item["run_id"] == run_id)
        self.assertEqual(row["io_service_avg_us"], "7.000000")
        self.assertEqual(row["io_service_count"], "2")

    def test_depth_summary_aggregates_stall_count(self) -> None:
        batch_id = "batch-20260831T015921Z"
        self.make_batch(self.root, batch_id)
        expected = {2: (570.0, 5.0), 4: (153.0, 0.0), 8: (80.0, 0.0), 16: (43.0, 0.0), 32: (22.0, 0.0)}
        for depth, (mean, _sd) in expected.items():
            for repetition in (1, 2, 3):
                interval_path = self.root / f"{batch_id}-d{depth:02d}-r{repetition}" / "interval.json"
                interval = json.loads(interval_path.read_text())
                interval["stall_count"] = int(mean - 5 + (repetition - 1) * 5) if depth == 2 else int(mean)
                interval_path.write_text(json.dumps(interval))

        result = self.run_generator("--batch", batch_id, "--runs-root", str(self.root), "--summary-dir", str(self.summary))

        self.assertEqual(result.returncode, 0, result.stderr)
        with (self.summary / "depth-summary.csv").open() as stream:
            rows = {int(row["depth"]): row for row in csv.DictReader(stream)}
        self.assertEqual(rows[2]["stall_count_mean"], "570.000000")
        self.assertEqual(rows[2]["stall_count_sd"], "5.000000")
        for depth in (4, 8, 16, 32):
            self.assertEqual(rows[depth]["stall_count_sd"], "0.000000")

    def test_batch_directory_rejects_mixed_batches(self) -> None:
        batch_dir = Path(self.temp_dir.name) / "batch-directory"
        batch_dir.mkdir()
        self.make_run(batch_dir, "batch-20260831T015921Z-d02-r1", 2, 1)
        self.make_run(batch_dir, "batch-20260831T020000Z-d02-r1", 2, 1)

        result = self.run_generator(
            "--batch-dir",
            str(batch_dir),
            "--runs-root",
            str(self.root),
            "--summary-dir",
            str(self.summary),
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("mixed batches", result.stderr)

    def test_batch_directory_rejects_malformed_run_name(self) -> None:
        batch_id = "batch-20260831T015921Z"
        batch_dir = Path(self.temp_dir.name) / batch_id
        batch_dir.mkdir()
        self.make_batch(batch_dir, batch_id)
        (batch_dir / f"{batch_id}-d02-rx").mkdir()

        result = self.run_generator(
            "--batch-dir",
            str(batch_dir),
            "--summary-dir",
            str(self.summary),
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("malformed run IDs", result.stderr)

    def test_batch_selection_rejects_zero_runs(self) -> None:
        result = self.run_generator(
            "--batch",
            "batch-20260831T015921Z",
            "--runs-root",
            str(self.root),
            "--summary-dir",
            str(self.summary),
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing", result.stderr)

    def test_batch_selection_rejects_missing_artifact(self) -> None:
        batch_id = "batch-20260831T015921Z"
        self.make_batch(self.root, batch_id)
        (self.root / f"{batch_id}-d02-r1" / "metrics.json").unlink()

        result = self.run_generator(
            "--batch",
            batch_id,
            "--runs-root",
            str(self.root),
            "--summary-dir",
            str(self.summary),
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid runs", result.stderr)


if __name__ == "__main__":
    unittest.main()
