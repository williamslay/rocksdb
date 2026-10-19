# Experimental pwrite Tracing

These tools collect Linux `pwrite64` syscall and scheduler data for a short, explicitly selected process. They do not start RocksDB, YCSB, or any benchmark. They do not run automatically.

## Requirements

- Linux with the `syscalls:sys_enter_pwrite64`, `syscalls:sys_exit_pwrite64`, and `sched:sched_switch` tracepoints.
- `bpftrace` 0.20.2 or a compatible release.
- Permission to attach the selected tracepoints, kprobes, and uprobes. Most systems require elevated privileges.
- For compaction-scoped tracing, an unstripped RocksDB shared library that exports `rocksdb::CompactionJob::Run()` under the expected symbol name.

The compaction tracer accepts target TGID as `$1` and shared-library path as `$2`. The library path is supplied on the command line. The script contains no host-specific path or PID.

The argument form follows the [bpftrace 0.20.2 positional-parameter documentation](https://github.com/bpftrace/bpftrace/blob/v0.20.2/docs/language.md). The exact dynamic uprobe path form was not executed in this change. The [0.20.2 manual](https://github.com/bpftrace/bpftrace/blob/v0.20.2/man/adoc/bpftrace.adoc) says `--dry-run` attaches probes, so validation did not use it.

## Validate syscall tracing with a small client

Use a disposable output path with at least 16 KiB available. The client creates one new 16 KiB file and refuses to overwrite an existing file.

1. In terminal 1, start the client:

   ```bash
   python3 tools/experimental/pwrite-tracing/smoke_client.py /tmp/pwrite-smoke.bin
   ```

2. Copy the printed TGID. In terminal 2, attach the smoke tracer:

   ```bash
   sudo bpftrace tools/experimental/pwrite-tracing/pwrite_smoke.bt TGID
   ```

3. After bpftrace reports that it attached its probes, press Enter in terminal 1.
4. Confirm four `pwrite` rows, each with `requested=4096` and `return=4096`. Stop bpftrace with Ctrl-C and check `DIAG` counters.

This smoke test checks syscall probe attachment and entry/exit pairing. It does not check compaction uprobes or scheduler/I/O-wait probe coverage. The client writes only after the operator releases it.

## Trace an existing RocksDB process

Choose an already-running RocksDB process. Do not start a workload as part of attaching the tracer. Set `TGID` to the process ID and `LIBROCKSDBJNI` to the exact shared library loaded by that process.

```bash
sudo bpftrace tools/experimental/pwrite-tracing/pwrite_compaction.bt \
  "$TGID" -- "$LIBROCKSDBJNI"
```

The tracer records `pwrite64` calls only while the target process is inside `CompactionJob::Run()`. It also records job-function entry and exit intervals. These uprobes identify thread and time; they do not expose a RocksDB job ID. If the binary is stripped or uses a different symbol, the uprobe will not attach.

Use a bounded capture when appropriate, for example by wrapping the command in `timeout`. Review the host's tracing permissions and the target process before attaching. Tracing overhead is workload-dependent and is not measured by these scripts.

## Output fields

`pwrite_compaction.bt` emits `pwrite` rows with these columns:

| Column | Meaning |
|---|---|
| `enter_ns`, `exit_ns` | bpftrace monotonic timestamps |
| `tid` | Thread ID issuing the syscall |
| `fd`, `offset` | File descriptor and requested file offset |
| `requested`, `return` | Requested byte count and syscall return value |
| `wall_ns` | Elapsed time between syscall entry and exit |
| `offcpu_ns` | Scheduler-observed off-CPU time inside the syscall interval |
| `offcpu_state0_ns`, `offcpu_state_nonzero_ns` | Off-CPU time grouped by raw `sched_switch` previous state being zero or nonzero |
| `io_wait_proxy_ns` | Off-CPU time overlapping a traced `io_schedule` scope while previous state is nonzero |
| `job_depth` | Nested `CompactionJob::Run()` depth on the thread; not a job identifier |

`pwrite_smoke.bt` emits syscall timing and return values without compaction or scheduler probes. Both scripts print diagnostic counters at exit. Nonzero unmatched, duplicate, underflow, or right-censored counters require investigation before interpreting a capture.

## Interpretation limits

- `wall_ns` measures syscall wall time. It is not pure device service time.
- `offcpu_ns` is scheduler-derived. It can include waits unrelated to storage.
- `io_wait_proxy_ns` is a proxy based on scheduler state and `io_schedule` function scopes. It is not direct device latency, physical-media time, or NAND latency.
- Whole-device `iostat` data provide context only. They do not attribute device activity to RocksDB.
- A short capture describes only its observed interval. It does not establish full-run behavior or tracer overhead.
- The scripts do not collect experiment results, launch workloads, or remove output files.
