#!/usr/bin/env -S uv run --script
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
# --- How to run ---
# 1. Install uv (if not installed): curl -LsSf https://astral.sh/uv/install.sh | sh
# 2. Run: uv run tools/compaction_io_experiment_summary.py --batch=batch-YYYYMMDDTHHMMSSZ
# 3. Or make executable and run: chmod +x tools/compaction_io_experiment_summary.py && ./tools/compaction_io_experiment_summary.py
# ------------------

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Final, Sequence, assert_never

if __package__:
    from .compaction_io_experiment_summary_data import (
        DepthStats,
        Rejection,
        Run,
        load_run,
        relative_delta,
        summarize,
    )
    from .compaction_io_experiment_summary_output import (
        write_csv_outputs,
        write_receipt,
        write_report,
    )
else:
    from compaction_io_experiment_summary_data import (
        DepthStats,
        Rejection,
        Run,
        load_run,
        relative_delta,
        summarize,
    )
    from compaction_io_experiment_summary_output import (
        write_csv_outputs,
        write_receipt,
        write_report,
    )

REPO: Final = Path(__file__).resolve().parents[1]
RUNS: Final = REPO / "results/isolated-experiment/runs"
SUMMARY: Final = REPO / "results/isolated-experiment/summary"
DEPTHS: Final = (2, 4, 8, 16, 32)
FRESH_BATCH_ID: Final = re.compile(r"batch-\d{8}T\d{6}Z")
FRESH_RUN_ID: Final = re.compile(
    r"(?P<batch>batch-\d{8}T\d{6}Z)-d(?P<depth>\d{2})-r(?P<repetition>[1-3])"
)


class SummarySelectionError(ValueError):
    pass


def decision(
    stats: list[DepthStats], accepted_count: int, rejected_count: int
) -> tuple[str, str, list[str]]:
    counts = {item.depth: item.count for item in stats}
    lines = [
        f"Accepted runs: {accepted_count}",
        f"Rejected runs: {rejected_count}",
        "Counts by depth: "
        + ", ".join(f"{depth}={counts.get(depth, 0)}" for depth in DEPTHS),
    ]
    if any(counts.get(depth, 0) < 3 for depth in DEPTHS):
        return "inconclusive", "no observed knee", lines + [
            "Required three-run-per-depth matrix is incomplete"
        ]

    by_depth = {item.depth: item for item in stats}
    baseline = by_depth[2]
    best = min(stats, key=lambda item: item.e_io_mean)
    wall_min = min(item.t_compaction_mean_us for item in stats)
    wall_max = max(item.t_compaction_mean_us for item in stats)
    wall_insensitive = (wall_max - wall_min) / max(wall_min, 1000.0) <= 0.05
    work_comparable = all(
        relative_delta(
            item.cpu_mean_us / item.input_mean_bytes,
            baseline.cpu_mean_us / baseline.input_mean_bytes,
        )
        <= 0.10
        and relative_delta(item.input_mean_bytes, baseline.input_mean_bytes) <= 0.10
        and relative_delta(item.output_mean_bytes, baseline.output_mean_bytes) <= 0.10
        for item in stats
    )
    read_amp_stable = all(
        relative_delta(item.read_amp_mean, baseline.read_amp_mean) <= 0.10
        for item in stats
    )
    device_not_saturated = all(
        item.max_input_util < 90 and item.max_output_util < 90 for item in stats
    )
    exposed_reduction = (baseline.t_exposed_mean_us - best.t_exposed_mean_us) / max(
        abs(baseline.t_exposed_mean_us), 1e-12
    )
    eio_reduction = (baseline.e_io_mean - best.e_io_mean) / max(
        abs(baseline.e_io_mean), 1e-12
    )
    wall_reduction = (baseline.t_compaction_mean_us - best.t_compaction_mean_us) / max(
        baseline.t_compaction_mean_us, 1000.0
    )
    read_below_envelope = False
    if all(item.e_io_mean < 0.01 for item in stats) and wall_insensitive:
        classification = "negative"
    elif (
        exposed_reduction >= 0.10
        and eio_reduction >= 0.10
        and wall_reduction >= 0.05
        and work_comparable
        and read_amp_stable
        and read_below_envelope
        and device_not_saturated
    ):
        classification = "positive"
    else:
        classification = "inconclusive"

    knee = "no observed knee"
    for first, following in zip(stats, stats[1:]):
        reduction = (first.e_io_mean - following.e_io_mean) / max(
            abs(first.e_io_mean), 1e-12
        )
        if reduction < 0.05 and (
            following.t_compaction_mean_us >= first.t_compaction_mean_us
            or following.read_amp_mean > first.read_amp_mean
            or following.max_input_util > first.max_input_util
            or following.max_output_util > first.max_output_util
        ):
            knee = str(first.depth)
            break
    lines.extend(
        (
            f"Best depth by mean E_io: {best.depth}",
            f"T_exposed_io reduction depth 2 to best: {exposed_reduction:.2%}",
            f"E_io reduction depth 2 to best: {eio_reduction:.2%}",
            f"Wall-time reduction depth 2 to best: {wall_reduction:.2%}",
            f"wall_insensitive={wall_insensitive}",
            f"work_comparable={work_comparable}",
            f"read_amp_stable={read_amp_stable}",
            f"device_not_saturated={device_not_saturated}",
            f"read_below_envelope={read_below_envelope} (no existing envelope artifact)",
        )
    )
    if knee == "no observed knee":
        lines.append("depth64_required=True before declaring no observed knee")
    return classification, knee, lines


def _batch_id(value: str) -> str:
    if FRESH_BATCH_ID.fullmatch(value) is None:
        raise SummarySelectionError(f"invalid fresh batch ID: {value}")
    return value


def select_batch(
    batch: str | None, batch_dir: Path | None, runs_root: Path
) -> tuple[str, list[Path]]:
    if batch is not None:
        candidate = Path(batch)
        if candidate.is_dir():
            batch_id = _batch_id(candidate.name)
            direct_runs = any(
                path.is_dir() and FRESH_RUN_ID.fullmatch(path.name) is not None
                for path in candidate.iterdir()
            )
            root = candidate if direct_runs else runs_root
        else:
            batch_id = _batch_id(batch)
            root = runs_root
    else:
        if batch_dir is None or not batch_dir.is_dir():
            raise SummarySelectionError("batch directory does not exist")
        root = batch_dir
        child_batches = {
            match["batch"]
            for path in root.iterdir()
            if path.is_dir()
            for match in [FRESH_RUN_ID.fullmatch(path.name)]
            if match is not None
        }
        if len(child_batches) > 1:
            raise SummarySelectionError("batch directory contains mixed batches")
        if child_batches:
            batch_id = child_batches.pop()
        else:
            batch_id = _batch_id(root.name)
            root = runs_root

    expected = {
        f"{batch_id}-d{depth:02d}-r{repetition}"
        for depth in DEPTHS
        for repetition in (1, 2, 3)
    }
    if not root.is_dir():
        raise SummarySelectionError(f"runs directory does not exist: {root}")
    paths = [root / name for name in sorted(expected)]
    missing = [path.name for path in paths if not path.is_dir()]
    if missing:
        raise SummarySelectionError(f"batch is incomplete; missing: {', '.join(missing)}")
    unexpected = [
        path.name
        for path in root.iterdir()
        if path.is_dir() and path.name.startswith(batch_id + "-") and path.name not in expected
    ]
    if unexpected:
        raise SummarySelectionError(f"malformed run IDs: {', '.join(sorted(unexpected))}")
    return batch_id, paths


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--batch")
    selector.add_argument("--batch-dir", type=Path)
    parser.add_argument("--runs-root", type=Path, default=RUNS)
    parser.add_argument("--summary-dir", type=Path, default=SUMMARY)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        batch_id, paths = select_batch(args.batch, args.batch_dir, args.runs_root)
        accepted: list[Run] = []
        rejected: list[Rejection] = []
        for path in paths:
            outcome = load_run(path, batch_id)
            match outcome:
                case Run() as run:
                    accepted.append(run)
                case Rejection() as rejection:
                    rejected.append(rejection)
                case unreachable:
                    assert_never(unreachable)
        if rejected:
            raise SummarySelectionError(
                f"batch contains invalid runs: {rejected[0].run_id}: {rejected[0].reason}"
            )
        if not accepted:
            raise SummarySelectionError(f"batch contains zero valid runs: {batch_id}")
        args.summary_dir.mkdir(parents=True, exist_ok=True)
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    stats = [summarize(depth, [run for run in accepted if run.depth == depth]) for depth in DEPTHS]
    write_csv_outputs(accepted, stats, rejected, args.summary_dir)
    classification, knee, lines = decision(stats, len(accepted), len(rejected))
    write_report(args.summary_dir, batch_id, paths, classification, knee, lines)
    write_receipt(
        args.summary_dir, batch_id, paths, len(accepted), len(rejected), classification, knee
    )
    print("accepted", len(accepted), "rejected", len(rejected), "classification", classification)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
