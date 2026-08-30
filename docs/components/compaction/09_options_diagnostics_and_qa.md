# 09 选项、诊断与 QA

本章是 compaction 选项和证据的唯一 owner。选项决定入口、候选选择、并发、输入输出和服务边界，但不替代调用链。阅读入口与 picker 时，分别参见 `01_entry_and_scheduling.md`、`02_scores_and_input_picking.md`、`03_compaction_styles.md`。

## 选项到代码

下表只列 public API 或明确的工具旗标。`max_compactions` 不在表中，它是由调度状态计算出的 internal state，不是 public option。

| 分组 | public option 或 flag | 声明 | 主要 consumer | 语义边界 |
| --- | --- | --- | --- | --- |
| trigger | `level0_file_num_compaction_trigger`, `periodic_compaction_seconds`, `disable_auto_compactions` | `include/rocksdb/advanced_options.h` | `VersionStorageInfo::ComputeCompactionScore`, `MaybeScheduleFlushOrCompaction` | 触发候选，不保证立即执行 |
| trigger | `compact_files_one_in`, `compact_range_one_in`, `compact_range_width` | `db_stress_tool/db_stress_gflags.cc` | `db_stress_tool/db_stress_test_base.cc` | db_stress workload knobs，非 DB public options |
| picker | `compaction_style`, `compaction_pri`, `level_compaction_dynamic_level_bytes` | `include/rocksdb/advanced_options.h`, `tools/db_bench_tool.cc` | `ColumnFamilyData::PickCompaction`, style pickers | dispatch 和候选排序；tiered 是 proximal/per-key placement 证据，不是第五种 picker |
| picker | `CompactionOptionsUniversal`, `CompactionOptionsFIFO` | `include/rocksdb/universal_compaction.h`, `include/rocksdb/advanced_options.h` | `UniversalCompactionPicker`, `FIFOCompactionPicker` | style-specific 输入和淘汰规则；不据此给出 universal tuning 值 |
| concurrency | `max_background_jobs`, `max_background_compactions`, `max_subcompactions` | `include/rocksdb/options.h` | `DBImpl::MaybeScheduleFlushOrCompaction`, `CompactionJob::Run` | worker 数和 job 内切分；实际可用并发仍受调度与冲突约束 |
| I/O | `compaction_readahead_size`, `use_direct_io_for_compaction_reads` | `include/rocksdb/options.h`, `tools/db_bench_tool.cc` | `CompactionJob::CreateInputIterator`, `Version::MakeInputIterator`, `TableCache::NewIterator` | SST 内部 readahead、direct I/O、block cache 是不同机制 |
| I/O | `blob_compaction_readahead_size` | `tools/db_bench_tool.cc`；blob 选项见 `include/rocksdb/advanced_options.h` | blob compaction reader/fetcher | 只描述 blob 读取路径，不能归入 SST `COMPACTION_PREFETCH_BYTES` |
| output | `max_compaction_bytes`, `target_file_size_base`, `compression` | `include/rocksdb/advanced_options.h` | picker input scope、`CompactionJob::Run`, output builder | 影响候选上限或文件生成，不是性能保证 |
| service | `CompactionService`, `OpenAndCompactOptions::allow_resumption` | `include/rocksdb/options.h` | `CompactionServiceCompactionJob`, `DB::OpenAndCompact` | remote/resume 最终仍由本地安装路径接管 |
| service | `openandcompact_allow_resumption`, `openandcompact_test_cancel_on_odd`, `openandcompact_cancel_after_millseconds` | `tools/db_bench_tool.cc` | `Benchmark::OpenAndCompact`, `OpenAndCompactOptions` | db_bench 测试控制；拼写 `millseconds` 按源码保留 |
| blob | `enable_blob_files`, `min_blob_size`, `blob_file_size`, `blob_compression_type`, `enable_blob_garbage_collection`, `blob_garbage_collection_age_cutoff`, `blob_garbage_collection_force_threshold`, `blob_compaction_readahead_size`, `blob_file_starting_level`, `prepopulate_blob_cache` | `include/rocksdb/advanced_options.h` | blob builder、fetcher、garbage accounting | blob 文件生成、读取、回收分属不同阶段 |

`compaction_readahead_size` 当前声明默认值是 `2 * 1024 * 1024`。这不是推荐值。`fill_cache=false` 只表示不插入 block cache，不能推出完全跳过已有 lookup。`COMPACTION_PREFETCH_BYTES` 只用于内部 compaction prefetch 计数，不能代表所有 OS、page-cache、SST 或 blob readahead。

`max_compactions` 的正确读法是 computed scheduling state。不要把它写成可由用户直接设置的 public option，也不要用它替代 `max_background_compactions` 的 API 说明。

## 诊断顺序

1. 先确认入口和 reason。日志、`CompactionReason`、`CompactionJobInfo` 可回答是谁触发、是否 manual、是否完成。`include/rocksdb/listener.h` 的 `OnCompactionBegin`、`OnCompactionPreCommit`、`OnCompactionCompleted` 是事件边界。
2. 再看 picker。将 `VersionStorageInfo` 的 score、候选文件、overlap、in-progress conflict 与目标 level 对照，不能只看文件数。
3. 再看 job。`CompactionJobStats` 和 `include/rocksdb/statistics.h` 的 compaction counters 分开回答输入、输出和时间。`COMPACTION_PREFETCH_BYTES` 只回答内部 prefetch。
4. 最后看 I/O。区分 direct I/O、filesystem/page-cache prefetch、`FilePrefetchBuffer`、block cache 与 blob readahead。需要把 workload、option、reason、job info 和 counter 放在同一份记录中。
5. 用测试锚点复现语义，不把单个成功日志当作证明。测试只证明它覆盖的输入和断言。

## 测试锚点矩阵

| 证据家族 | 路径 | 适合回答 |
| --- | --- | --- |
| picker | `db/compaction/compaction_picker_test.cc` | level、universal、FIFO 的候选和阻塞 |
| job/filter | `db/compaction/compaction_picker_test.cc`, `db/db_compaction_filter_test.cc` | picker 候选/阻塞与 filter 保留/删除语义 |
| manual | `db/manual_compaction_test.cc`, `db/compact_files_test.cc` | `CompactRange`、`CompactFiles`、冲突和取消 |
| service/resume | `db/compaction/compaction_service_test.cc` | service protocol、fallback、resumable output |
| blob | `db/blob/db_blob_compaction_test.cc`, `db/compaction/compaction_job_test.cc` | blob 读取、输出和 garbage accounting |
| timestamp/filter | `db/db_compaction_filter_test.cc`, `db/compaction/compaction_iterator_test.cc` | timestamp、filter 时序和保留条件 |
| tiered | `db/compaction/tiered_compaction_test.cc` | proximal/per-key placement 行为；不是第五种 picker |
| example | `examples/compact_files_example.cc` | public `CompactFiles` 调用入口 |

相关 `db_stress` compaction knobs 在 `db_stress_tool/db_stress_gflags.cc` 定义，在 `db_stress_tool/db_stress_test_base.cc` 消费。相关 bench flags 在 `tools/db_bench_tool.cc` 定义并由 `Benchmark::OpenAndCompact` 等路径消费。它们适合建立触发、取消和压力场景，不产生普适调优结论。

## 代表性命令

以下全部是文档示例，未执行。示例不构成验证，也不提供 universal values。

示例，未执行
```text
./db_stress --compact_files_one_in=100 --compact_range_one_in=100 --compact_range_width=10000
```

示例，未执行
```text
./db_bench --benchmarks=openandcompact --openandcompact_allow_resumption=true --openandcompact_test_cancel_on_odd=true --openandcompact_cancel_after_millseconds=1
```

示例，未执行
```text
./db_bench --benchmarks=fillrandom,compact --compaction_readahead_size=2097152 --blob_compaction_readahead_size=0
```

## Misconception checklist

- `max_compactions` 是 internal computed state，不是 public option。
- L0 不保证总是直接到 L1，dynamic base level 和 picker 状态会改变目标。
- tiered test 是 proximal/per-key 行为证据，不是第五种 picker。
- `fill_cache=false` 不保证没有已有 block-cache lookup。
- 所有 prefetch bytes 不等于 `COMPACTION_PREFETCH_BYTES`。
- `trim_ts` 与 `full_history_ts_low` 是两条不同 timestamp 路径。
- remote service 不等于完整本地 `CompactionJob::Run` 验证，安装仍有本地 owner。
- `openandcompact_allow_resumption` 不是普通 compaction 的失败重试开关。
- SST readahead 与 blob readahead 不共享 owner、buffer 或 counter。
- 一次测试通过、一个日志出现或一个 counter 增长，都不能推出 measured performance 或 universal tuning value。

## QA 边界与 adversarial probes

本 docs-only 任务只做 Python 3 standard-library structural checks，不运行 build、test、benchmark、Jekyll、`db_stress` 或 `db_bench`。Manual QA 读取本章，并对 exact flag definitions 和 consumers 执行 CodeGraph/source queries，结果记录在 evidence。

逐条 claim、source path、symbol 和 text-check 记录见 `.omo/evidence/rocksdb-compaction-code-guide/task-9-claims.tsv`；范围与 baseline 记录见 `.omo/evidence/rocksdb-compaction-code-guide/task-9-scope_receipt.txt`，malformed-fixture 的实际非零 receipt 见 `.omo/evidence/rocksdb-compaction-code-guide/task-9-malformed-receipt.txt`。

| probe | 结果 | 原因 |
| --- | --- | --- |
| dirty_worktree | PASS | 只新增本章、evidence 和 task-specific fixture，保留其他 dirty paths |
| stale_state | PASS | source claims 以当前 CodeGraph/source query 为准，未依赖旧生成物 |
| misleading_success_output | PASS | 示例统一标记 `示例，未执行`，不把输出当 validation |
| malformed input | PASS | balanced negative fixture 含 forbidden public-option 和 universal-tuning claims，checker 必须非零退出 |
| prompt-injection | N/A | 文档和源码无外部指令输入面 |
| generated-artifact | N/A | 未修改 generated C API、BUCK 或其他生成文件 |
| cancel/resume | N/A | 仅记录现有 option、consumer 和测试锚点，未执行运行时场景 |
| long-command | N/A | 未启动命令，也未执行 stress/bench |
