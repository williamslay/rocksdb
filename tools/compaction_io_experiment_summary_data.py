#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

import json, re, statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Final, TypeAlias

JsonValue: TypeAlias = (
    None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
)
JsonObject: TypeAlias = dict[str, JsonValue]


class SummaryDataValueError(ValueError):
    pass


FRESH_RUN_ID: Final = re.compile(
    r"(?P<batch>batch-\d{8}T\d{6}Z)-d(?P<depth>\d{2})-r(?P<repetition>[1-3])"
)
LEGACY_RUN_ID: Final = re.compile(r"uring_d(?P<depth>\d+)_r(?P<repetition>\d+)")


@dataclass(frozen=True, slots=True)
class DeviceStats:
    samples: int
    max_read_mb_s: float
    max_write_mb_s: float
    max_read_await_ms: float
    max_write_await_ms: float
    max_queue_size: float
    max_util_percent: float


@dataclass(frozen=True, slots=True)
class Run:
    run_id: str
    depth: int
    repetition: int
    t_compaction_us: float
    cpu_time_us: float
    t_exposed_io_us: float
    overlap_total_us: float
    stall_count: int
    physical_request_count: int
    physical_read_bytes: float
    prefetched_bytes: float
    logical_input_bytes: float
    output_bytes: float
    read_amplification: float
    io_service_count: int
    io_service_avg_us: float
    io_service_stddev_us: float
    io_service_p95_us: float
    io_service_p99_us: float
    input_device: int
    output_device: int
    input_stats: DeviceStats
    output_stats: DeviceStats


@dataclass(frozen=True, slots=True)
class Rejection:
    run_id: str
    reason: str
    evidence_path: str


@dataclass(frozen=True, slots=True)
class DepthStats:
    depth: int
    count: int
    t_compaction_mean_us: float
    t_compaction_sd_us: float
    t_exposed_mean_us: float
    t_exposed_sd_us: float
    stall_count_mean: float
    stall_count_sd: float
    e_io_mean: float
    e_io_sd: float
    cpu_mean_us: float
    cpu_sd_us: float
    input_mean_bytes: float
    input_sd_bytes: float
    output_mean_bytes: float
    output_sd_bytes: float
    read_amp_mean: float
    read_amp_sd: float
    max_input_util: float
    max_output_util: float


def load_json(path: Path) -> JsonObject:
    raw = json.loads(path.read_text())
    if not isinstance(raw, dict):
        raise SummaryDataValueError(f"JSON root is not an object: {path}")
    return {str(key): value for key, value in raw.items()}


def nested(payload: JsonObject, key: str) -> JsonObject:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise SummaryDataValueError(f"missing JSON object {key}")
    return {str(name): item for name, item in value.items()}


def number(payload: JsonObject, key: str) -> float:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SummaryDataValueError(f"missing numeric field {key}")
    return float(value)


def device_file(path: Path) -> int:
    devices = {
        int(line.split()[0]) for line in path.read_text().splitlines() if line.strip()
    }
    if len(devices) != 1:
        raise SummaryDataValueError(f"ambiguous device file: {path}")
    return devices.pop()


def iostat(path: Path, device: str) -> DeviceStats:
    rows: list[tuple[float, ...]] = []
    for line in path.read_text().splitlines():
        fields = line.split()
        if len(fields) < 23 or fields[0] != device:
            continue
        try:
            rows.append(
                (
                    float(fields[2]) / 1024,
                    float(fields[8]) / 1024,
                    float(fields[5]),
                    float(fields[11]),
                    float(fields[21]),
                    float(fields[22]),
                )
            )
        except ValueError:
            continue
    if not rows:
        raise SummaryDataValueError(f"no iostat samples for {device}")
    return DeviceStats(
        len(rows),
        max(row[0] for row in rows),
        max(row[1] for row in rows),
        max(row[2] for row in rows),
        max(row[3] for row in rows),
        max(row[4] for row in rows),
        max(row[5] for row in rows),
    )


def load_run(path: Path, expected_batch: str | None = None) -> Run | Rejection:
    match = FRESH_RUN_ID.fullmatch(path.name)
    if match is None and expected_batch is None:
        match = LEGACY_RUN_ID.fullmatch(path.name)
    if match is None:
        return Rejection(path.name, "unrecognized run ID", str(path))
    run_id = path.name
    depth = int(match["depth"])
    repetition = int(match["repetition"])
    batch = match.groupdict().get("batch")
    if expected_batch is not None and batch != expected_batch:
        return Rejection(run_id, "run belongs to a different batch", str(path))
    if depth not in {2, 4, 8, 16, 32}:
        return Rejection(run_id, "unsupported depth", str(path))
    if repetition not in {1, 2, 3}:
        return Rejection(run_id, "unsupported repetition", str(path))
    status_path = path / "status.txt"
    try:
        status = status_path.read_text()
    except OSError as error:
        return Rejection(run_id, f"missing status: {error}", str(path))
    if "classification=accepted" not in status:
        reason = next(
            (line[2:] for line in status.splitlines() if line.startswith("- ")),
            "status lacks classification=accepted",
        )
        return Rejection(run_id, reason, str(status_path))
    try:
        interval = load_json(path / "interval.json")
        metrics = nested(load_json(path / "metrics.json"), "compaction_io_experiment")
        nested(metrics, "sync_fallback_read_us")
        service = nested(metrics, "io_service_us")
        logical_input = interval.get(
            "logical_input_bytes", interval.get("logical_consumed_bytes")
        )
        if isinstance(logical_input, bool) or not isinstance(
            logical_input, (int, float)
        ):
            raise SummaryDataValueError("missing logical input bytes")
        consumed = float(logical_input)
        prefetched = number(interval, "prefetched_bytes")
        if consumed <= 0:
            raise SummaryDataValueError("logical input bytes are zero")
        if (
            int(number(interval, "depth")) != depth
            or int(number(metrics, "depth")) != depth
        ):
            raise SummaryDataValueError("depth mismatch")
        if int(number(interval, "request_span_bytes")) != 1048576:
            raise SummaryDataValueError("request span is not 1 MiB")
        if int(number(interval, "outstanding_async_reads")) != 0:
            raise SummaryDataValueError("outstanding async reads remain")
        if (
            number(interval, "t_compaction_us") <= 0
            or number(interval, "cpu_time_us") <= 0
        ):
            raise SummaryDataValueError("missing compaction timing")
        input_device = device_file(path / "input-sst-before-device.txt")
        output_device = device_file(path / "output-sst-after-device.txt")
        if input_device != 66306 or output_device != 66309:
            raise SummaryDataValueError("SST device mismatch")
        input_stats = iostat(path / "iostat.raw", "nvme0n1")
        output_stats = iostat(path / "iostat.raw", "nvme1n1")
        return Run(
            run_id,
            depth,
            repetition,
            number(interval, "t_compaction_us"),
            number(interval, "cpu_time_us"),
            number(interval, "t_exposed_io_us"),
            number(interval, "overlap_total_us"),
            int(number(interval, "stall_count")),
            int(number(interval, "physical_request_count")),
            number(interval, "physical_read_bytes"),
            prefetched,
            consumed,
            number(interval, "output_bytes"),
             number(interval, "physical_read_bytes") / consumed,
            int(number(service, "count")),
            number(service, "avg"),
            number(service, "stddev"),
            number(service, "p95"),
            number(service, "p99"),
            input_device,
            output_device,
            input_stats,
            output_stats,
        )
    except (OSError, TypeError, ValueError, KeyError) as error:
        return Rejection(run_id, str(error), str(path))


def mean_sd(values: list[float]) -> tuple[float, float]:
    return statistics.fmean(values), statistics.stdev(values) if len(
        values
    ) > 1 else 0.0


def summarize(depth: int, runs: list[Run]) -> DepthStats:
    tcomp = mean_sd([run.t_compaction_us for run in runs])
    exposed = mean_sd([run.t_exposed_io_us for run in runs])
    stalls = mean_sd([run.stall_count for run in runs])
    eio = mean_sd([run.t_exposed_io_us / run.t_compaction_us for run in runs])
    cpu = mean_sd([run.cpu_time_us for run in runs])
    inputs = mean_sd([run.logical_input_bytes for run in runs])
    outputs = mean_sd([run.output_bytes for run in runs])
    amps = mean_sd([run.read_amplification for run in runs])
    return DepthStats(
        depth,
        len(runs),
        tcomp[0],
        tcomp[1],
        exposed[0],
        exposed[1],
        stalls[0],
        stalls[1],
        eio[0],
        eio[1],
        cpu[0],
        cpu[1],
        inputs[0],
        inputs[1],
        outputs[0],
        outputs[1],
        amps[0],
        amps[1],
        max(run.input_stats.max_util_percent for run in runs),
        max(run.output_stats.max_util_percent for run in runs),
    )


def relative_delta(value: float, baseline: float) -> float:
    return abs(value - baseline) / max(abs(baseline), 1.0)
