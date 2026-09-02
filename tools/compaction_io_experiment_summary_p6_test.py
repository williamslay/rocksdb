#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

import csv
import json

from tools.compaction_io_experiment_summary_test import SummaryGeneratorTest


class SummaryP6Test(SummaryGeneratorTest):
    def test_read_amplification_uses_physical_bytes(self) -> None:
        batch_id = "batch-20260831T015921Z"
        run_id = f"{batch_id}-d02-r1"
        self.make_batch(self.root, batch_id)
        interval_path = self.root / run_id / "interval.json"
        interval = json.loads(interval_path.read_text())
        interval["physical_read_bytes"] = 2048.0
        interval_path.write_text(json.dumps(interval))

        result = self.run_generator(
            "--batch", batch_id, "--runs-root", str(self.root), "--summary-dir", str(self.summary)
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        with (self.summary / "per-run.csv").open() as stream:
            rows = list(csv.DictReader(stream))
        row = next(item for item in rows if item["run_id"] == run_id)
        self.assertEqual(row["read_amplification"], "2.000000")

    def test_report_and_receipt_list_raw_evidence(self) -> None:
        batch_id = "batch-20260831T015921Z"
        self.make_batch(self.root, batch_id)

        result = self.run_generator(
            "--batch", batch_id, "--runs-root", str(self.root), "--summary-dir", str(self.summary)
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        report = (self.summary.parent / "report.md").read_text()
        self.assertIn(f"{batch_id}-d02-r1/interval.json", report)
        self.assertIn("read_below_envelope=False", report)
        receipt = self.summary.parent / "p6" / batch_id / "P6_RECEIPT.md"
        self.assertTrue(receipt.exists())
        self.assertIn("Accepted runs: 15", receipt.read_text())
