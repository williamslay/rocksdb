#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from __future__ import annotations

import csv
from pathlib import Path

if __package__:
    from .compaction_io_experiment_summary_data import DepthStats, Rejection, Run
else:
    from compaction_io_experiment_summary_data import DepthStats, Rejection, Run


def _write_csv(path: Path, headers: tuple[str, ...], rows: list[tuple[str, ...]]) -> None:
    with path.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(headers)
        writer.writerows(rows)


def _fmt(value: float) -> str:
    return f"{value:.6f}"


def _run_rows(runs: list[Run]) -> tuple[list[tuple[str, ...]], list[tuple[str, ...]]]:
    per_run: list[tuple[str, ...]] = []
    devices: list[tuple[str, ...]] = []
    for run in runs:
        values = (
            run.t_compaction_us,
            run.cpu_time_us,
            run.t_exposed_io_us,
            run.t_exposed_io_us / run.t_compaction_us,
            run.overlap_total_us,
            run.physical_read_bytes,
            run.prefetched_bytes,
            run.logical_input_bytes,
            run.output_bytes,
            run.read_amplification,
            run.io_service_avg_us,
            run.io_service_stddev_us,
            run.io_service_p95_us,
            run.io_service_p99_us,
            run.input_stats.max_read_mb_s,
            run.output_stats.max_write_mb_s,
            run.input_stats.max_util_percent,
            run.output_stats.max_util_percent,
        )
        per_run.append(
            tuple(map(str, (run.run_id, run.depth, run.repetition)))
            + tuple(_fmt(value) for value in values)
            + tuple(map(str, (run.stall_count, run.physical_request_count, run.io_service_count)))
        )
        for name, stats in (("nvme0n1", run.input_stats), ("nvme1n1", run.output_stats)):
            devices.append(
                (
                    run.run_id,
                    str(run.depth),
                    name,
                    str(stats.samples),
                    _fmt(stats.max_read_mb_s),
                    _fmt(stats.max_write_mb_s),
                    _fmt(stats.max_read_await_ms),
                    _fmt(stats.max_write_await_ms),
                    _fmt(stats.max_queue_size),
                    _fmt(stats.max_util_percent),
                )
            )
    return per_run, devices


def _depth_rows(stats: list[DepthStats]) -> list[tuple[str, ...]]:
    return [
        tuple(map(str, (item.depth, item.count)))
        + tuple(
            _fmt(value)
            for value in (
                item.t_compaction_mean_us,
                item.t_compaction_sd_us,
                item.t_exposed_mean_us,
                item.t_exposed_sd_us,
                item.stall_count_mean,
                item.stall_count_sd,
                item.e_io_mean,
                item.e_io_sd,
                item.cpu_mean_us,
                item.cpu_sd_us,
                item.input_mean_bytes,
                item.input_sd_bytes,
                item.output_mean_bytes,
                item.output_sd_bytes,
                item.read_amp_mean,
                item.read_amp_sd,
                item.max_input_util,
                item.max_output_util,
            )
        )
        for item in stats
    ]


def write_csv_outputs(
    runs: list[Run], stats: list[DepthStats], rejected: list[Rejection], summary_dir: Path
) -> None:
    per_run, devices = _run_rows(runs)
    _write_csv(
        summary_dir / "per-run.csv",
        ("run_id", "depth", "repetition", "t_compaction_us", "cpu_time_us", "t_exposed_io_us", "e_io", "overlap_total_us", "physical_read_bytes", "prefetched_bytes", "logical_input_bytes", "output_bytes", "read_amplification", "io_service_avg_us", "io_service_stddev_us", "io_service_p95_us", "io_service_p99_us", "input_max_read_mb_s", "output_max_write_mb_s", "input_max_util_percent", "output_max_util_percent", "stall_count", "physical_request_count", "io_service_count"),
        per_run,
    )
    _write_csv(
        summary_dir / "depth-summary.csv",
        ("depth", "N", "t_compaction_mean_us", "t_compaction_sd_us", "t_exposed_mean_us", "t_exposed_sd_us", "stall_count_mean", "stall_count_sd", "e_io_mean", "e_io_sd", "cpu_mean_us", "cpu_sd_us", "logical_input_mean_bytes", "logical_input_sd_bytes", "logical_output_mean_bytes", "logical_output_sd_bytes", "read_amplification_mean", "read_amplification_sd", "max_input_util_percent", "max_output_util_percent"),
        _depth_rows(stats),
    )
    _write_csv(
        summary_dir / "device-summary.csv",
        ("run_id", "depth", "device", "samples", "max_read_mb_s", "max_write_mb_s", "max_read_await_ms", "max_write_await_ms", "max_queue_size", "max_util_percent"),
        devices,
    )
    _write_csv(
        summary_dir / "rejected-runs.csv",
        ("run_id", "reason", "evidence_path"),
        [(item.run_id, item.reason, item.evidence_path) for item in rejected],
    )


def write_report(
    summary_dir: Path,
    batch_id: str,
    paths: list[Path],
    classification: str,
    knee: str,
    decision_lines: list[str],
) -> None:
    report = [
        "# Isolated compaction input/output experiment",
        "",
        f"Batch: `{batch_id}`",
        "Generated only from raw accepted run artifacts.",
        "",
        "## Decision",
        f"Classification: **{classification}**",
        f"Knee: **{knee}**",
        "",
        *decision_lines,
        "",
        "## Raw evidence",
    ]
    report.extend(
        f"- `{path / filename}`"
        for path in paths
        for filename in ("status.txt", "interval.json", "metrics.json", "iostat.raw")
    )
    report.extend(
        (
            "",
            "## Definitions",
            "- `E_io = T_exposed_io / T_compaction` per run.",
            "- Arithmetic means and sample standard deviations use independent completed runs; `N` is run count.",
            "- Read amplification is `physical_read_bytes / logical_consumed_bytes`; prefetched bytes remain separate evidence.",
            "- Positive classification requires predeclared comparability, stability, device, wall-time, and read-envelope gates.",
            "- No CMM-H, Cylon, depth64, or expanded depth/request-size search was performed.",
        )
    )
    (summary_dir.parent / "report.md").write_text("\n".join(report) + "\n")


def write_receipt(
    summary_dir: Path,
    batch_id: str,
    paths: list[Path],
    accepted_count: int,
    rejected_count: int,
    classification: str,
    knee: str,
) -> Path:
    receipt_dir = summary_dir.parent / "p6" / batch_id
    receipt_dir.mkdir(parents=True, exist_ok=True)
    receipt = [
        "# P6 Aggregation Receipt",
        "",
        "Status: PASS",
        f"Batch ID: `{batch_id}`",
        f"Accepted runs: {accepted_count}",
        f"Rejected runs: {rejected_count}",
        f"Classification: {classification}",
        f"Knee: {knee}",
        "",
        "## Generated outputs",
        f"- `{summary_dir / 'per-run.csv'}`",
        f"- `{summary_dir / 'depth-summary.csv'}`",
        f"- `{summary_dir / 'device-summary.csv'}`",
        f"- `{summary_dir / 'rejected-runs.csv'}`",
        f"- `{summary_dir.parent / 'report.md'}`",
        "",
        "## Raw evidence",
    ]
    receipt.extend(f"- `{path}`" for path in paths)
    receipt_path = receipt_dir / "P6_RECEIPT.md"
    receipt_path.write_text("\n".join(receipt) + "\n")
    return receipt_path
