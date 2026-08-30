# RocksDB Compaction

Compaction 将 LSM 中已写入的 memtable/SST/blob 状态重新组织为更少、更适合读取的文件。它同时承担候选调度、输入读取、键归约、输出生成、版本安装和 obsolete 文件清理。本文是导航入口；每个阶段的 canonical 细节只在对应 numbered chapter 中维护。

## 术语

| 术语 | 本指南中的含义 |
| --- | --- |
| entry / scheduling | 自动、`CompactRange`、`CompactFiles` 和 periodic 请求进入后台队列并获得执行机会。 |
| picker / candidate | 根据 score、marked、TTL、periodic、read-triggered 等状态形成可执行候选的选择层。 |
| clean cut / overlap | 以 user key 完整性和正在运行的 compaction 冲突为约束扩展输入范围。 |
| key reduction | 按 snapshot、sequence、timestamp、tombstone、merge、filter 和 blob resolver 决定保留或改写记录。 |
| output / installation | local 或 remote 产出并验证文件；`VersionSet::LogAndApply` 成功后才成为 live Version。 |
| resumable | 用持久化 internal-key cursor、完成 output metadata 和 counters 恢复；不是任意 partial file 续写。 |
| listener path | `NotifyOnCompactionBegin`、`OnCompactionPreCommit`、`OnCompactionCompleted` 的事件边界；callback 不应被假定在 DB mutex 内。 |

## 端到端地图与 ownership

```text
write/flush or manual API
  -> DBImpl enqueue/schedule
  -> ColumnFamilyData::PickCompaction
  -> CompactionPicker (level/universal/FIFO/null)
  -> CompactionJob input iterators + key reduction
  -> local subcompactions / remote service / resume
  -> verified SST/blob outputs
  -> CompactionJob::Install
  -> VersionSet::LogAndApply -> live Version
  -> obsolete-file purge and listener/statistics observation
```

| 阶段 | canonical owner | 交接结果 |
| --- | --- | --- |
| 请求与调度 | `DBImpl::EnqueuePendingCompaction`、`MaybeScheduleFlushOrCompaction` | 保持 CF 引用的 queued/running job；暂停、冲突和 shutdown 仍有效。 |
| 候选与输入范围 | `ColumnFamilyData::PickCompaction`、style picker、`CompactionPicker` | 带 input levels、output level、reason 和 clean-cut 范围的 `Compaction`。 |
| 输入 I/O | `CompactionJob::CreateInputIterator`、`VersionSet::MakeInputIterator`、`TableCache` | compaction caller、`fill_cache=false`、direct I/O/readahead 和 blob reader 语义。 |
| 键归约 | `CompactionIterator::NextFromInput`、range-del aggregator、merge/filter/blob resolver | 有序、可验证、符合 snapshot/timestamp/tombstone contract 的 output records。 |
| 本地/远程/恢复执行 | `CompactionJob`、`CompactionServiceCompactionJob`、resume helpers | verified metadata、paths、status 和 progress；remote 不直接安装 primary MANIFEST。 |
| 输出与安装 | `CompactionOutputs`、`CompactionJob::Run`、`CompactionJob::Install`、`VersionSet::LogAndApply` | sync/verify 后的候选文件；manifest durable 且 Version apply 成功才可见。 |
| 清理与观测 | `CleanupAbortedSubcompactions`、`PurgeObsoleteFiles`、compaction listener path | 物理删除、state cleanup、quarantine/pending protection、stats 和 begin/pre-commit/completed 事件。 |

## Exact 18-ID matrix

这是当前 checkout 的完整导航矩阵。每个 ID 只有一个 canonical chapter；source path、symbol 和 test/tool 是阅读锚点，不是运行结果。

| ID | chapter | source path | representative symbol | option/interface | test/tool anchor |
| --- | --- | --- | --- | --- | --- |
| `ENTRY_AUTO` | [01](01_entry_and_scheduling.md) | `db/db_impl/db_impl_compaction_flush.cc` | `DBImpl::MaybeScheduleFlushOrCompaction` | `DBOptions::max_background_jobs` | `db/db_test.cc:DBTest.AutomaticConflictsWithManualCompaction` |
| `ENTRY_RANGE` | [01](01_entry_and_scheduling.md) | `db/db_impl/db_impl_compaction_flush.cc` | `DBImpl::CompactRange` | `DB::CompactRange` | `db/db_compaction_test.cc:DBCompactionTest.ManualAutoRace` |
| `ENTRY_FILES` | [01](01_entry_and_scheduling.md) | `db/db_impl/db_impl_compaction_flush.cc` | `DBImpl::CompactFiles` | `DB::CompactFiles` | `examples/compact_files_example.cc` |
| `PICK_LEVEL` | [03](03_compaction_styles.md) | `db/compaction/compaction_picker_level.cc` | `LevelCompactionBuilder::PickCompaction` | `AdvancedColumnFamilyOptions::compaction_pri` | `db/compaction/compaction_picker_test.cc` |
| `PICK_UNIVERSAL` | [03](03_compaction_styles.md) | `db/compaction/compaction_picker_universal.cc` | `UniversalCompactionBuilder::PickCompaction` | `CompactionOptionsUniversal` | `db/compaction/compaction_picker_test.cc:UniversalSizeRatioTierCompactionLastLevel` |
| `PICK_FIFO` | [03](03_compaction_styles.md) | `db/compaction/compaction_picker_fifo.cc` | `FIFOCompactionPicker::PickCompaction` | `CompactionOptionsFIFO` | `db/compaction/compaction_picker_test.cc:NeedsCompactionFIFO` |
| `PICK_NULL` | [03](03_compaction_styles.md) | `db/compaction/compaction_picker.h` | `NullCompactionPicker::PickCompaction` | `disable_auto_compactions` | `db/compaction/compaction_picker.h` inline implementation |
| `CANDIDATES` | [02](02_scores_and_input_picking.md) | `db/compaction/compaction_picker.cc` | `CompactionPicker::ExpandInputsToCleanCut` | `AdvancedColumnFamilyOptions::max_compaction_bytes` | `db/compaction/compaction_picker_test.cc` |
| `IO_SST` | [04](04_input_io_and_memory.md) | `db/compaction/compaction_job.cc` | `CompactionJob::CreateInputIterator` | `DBOptions::use_direct_io_for_compaction_reads` | `db/compaction/compaction_job_test.cc` |
| `IO_BLOB` | [04](04_input_io_and_memory.md) | `db/compaction/compaction_iterator.cc` | `CompactionIterator::CreateBlobFetcherIfNeeded` | `AdvancedColumnFamilyOptions::blob_compaction_readahead_size` | `db/blob/db_blob_compaction_test.cc` |
| `SEMANTICS` | [05](05_key_reduction_semantics.md) | `db/compaction/compaction_iterator.cc` | `CompactionIterator::NextFromInput` | `CompactionFilter` and snapshot interface | `db/compaction/compaction_iterator_test.cc` |
| `EXEC_LOCAL` | [06](06_subcompactions_and_outputs.md) | `db/compaction/compaction_job.cc` | `CompactionJob::Run` | `DBOptions::max_subcompactions` | `db/compaction/compaction_job_test.cc` |
| `EXEC_REMOTE` | [07](07_remote_and_resumable.md) | `db/compaction/compaction_service_job.cc` | `CompactionServiceCompactionJob::Run` | `CompactionService` | `db/compaction/compaction_service_test.cc` |
| `EXEC_RESUME` | [07](07_remote_and_resumable.md) | `db/compaction/compaction_job.cc` | `CompactionJob::MaybeResumeSubcompactionProgressOnInputIterator` | `OpenAndCompactOptions::allow_resumption` | `db/compaction/compaction_service_test.cc` |
| `INSTALL` | [08](08_install_cleanup_and_observability.md) | `db/compaction/compaction_job.cc` | `CompactionJob::Install` | `VersionSet::LogAndApply` | `db/compaction/compaction_job_test.cc` |
| `CLEANUP` | [08](08_install_cleanup_and_observability.md) | `db/db_impl/db_impl_files.cc` | `DBImpl::PurgeObsoleteFiles` | `VersionEdit` file ownership interface | `db/db_test.cc` |
| `OBSERVABILITY` | [08](08_install_cleanup_and_observability.md) | `db/db_impl/db_impl_compaction_flush.cc` | `DBImpl::BuildCompactionJobInfo` | `EventListener::OnCompactionCompleted` | `db/compaction/compaction_job_test.cc` |
| `QA_TOOLS` | [09](09_options_diagnostics_and_qa.md) | `tools/db_bench_tool.cc` | `Benchmark::OpenAndCompact` | `openandcompact_allow_resumption` | `tools/db_stress_tool/db_stress_gflags.cc` |

## 不变量与常见误解

- 文件写完、sync 或 verify 成功，不等于 Version 已可见；必须经过 manifest durable 和 Version apply。
- L0 不总是到 L1；dynamic base level、picker style 和当前 Version 决定目标。
- `fill_cache=false` 禁止正常 admission，不禁止已有 block-cache lookup，也不代表零内存。
- tiered 是 proximal/per-key placement 边界，不是第五种 picker。
- `trim_ts` 与 `full_history_ts_low` 不可合并；timestamp/snapshot eligibility 先于 range-tombstone sequence comparison。
- remote service 不等于完整 local `CompactionJob::Run` verification，且不拥有 primary MANIFEST 安装。
- `CleanupCompaction` 是 state cleanup；普通 abort 的物理文件删除由 `CleanupAbortedSubcompactions` 负责，resumable abort 保留完成 output 和 progress。
- listener callback 不在 DB mutex 内执行；Completed 不能替代 PreCommit 的提交前 ownership 窗口。
- `COMPACTION_PREFETCH_BYTES` 不是所有 SST、OS page-cache、block-cache 或 blob readahead bytes 的总和。

## Chapters

| Chapter | File | Summary |
| --- | --- | --- |
| 1. 入口与调度 | [01_entry_and_scheduling.md](01_entry_and_scheduling.md) | 自动、手动、periodic 入口，队列、暂停、取消、重试和收尾。 |
| 2. 分数与输入选择 | [02_scores_and_input_picking.md](02_scores_and_input_picking.md) | score、candidate、overlap、clean cut、grandparents 和 trivial move 边界。 |
| 3. Compaction Style 算法 | [03_compaction_styles.md](03_compaction_styles.md) | leveled、universal、FIFO、null picker 及 proximal/per-key 边界。 |
| 4. 输入、I/O 与内存 | [04_input_io_and_memory.md](04_input_io_and_memory.md) | SST/blob 输入 iterator、cache、prefetch、direct I/O 和统计口径。 |
| 5. 键归约语义 | [05_key_reduction_semantics.md](05_key_reduction_semantics.md) | snapshot、sequence、timestamp、tombstone、merge、filter、blob 和 placement。 |
| 6. Local Subcompactions and Outputs | [06_subcompactions_and_outputs.md](06_subcompactions_and_outputs.md) | local job、subcompaction、边界、builder、输出验证和安装交接。 |
| 7. Remote 与可恢复 Compaction | [07_remote_and_resumable.md](07_remote_and_resumable.md) | service、secondary、fallback、progress、resume、取消和 retention。 |
| 8. 安装、清理与可观测性 | [08_install_cleanup_and_observability.md](08_install_cleanup_and_observability.md) | Version 安装、MANIFEST 原子可见性、cleanup、purge、listener、stats 和 IO。 |
| 9. 选项、诊断与 QA | [09_options_diagnostics_and_qa.md](09_options_diagnostics_and_qa.md) | option consumer、诊断顺序、测试锚点、误解和结构性 QA 边界。 |
| 10. Compaction Source Atlas | [10_source_atlas.md](10_source_atlas.md) | 完整 18-ID source/navigation atlas 与 boundary-only source families。 |

## Related Read/Write Docs

Compaction 输入读取复用 [SST File Lookup](../read_flow/05_sst_file_lookup.md)、[Block Cache Integration](../read_flow/06_block_cache.md)、[Iterator and Scan Path](../read_flow/07_iterator_scan.md) 和 [Prefetching and Async I/O](../read_flow/10_prefetching_and_async_io.md)。键语义边界复用 [SuperVersion and Snapshot Isolation](../read_flow/03_superversion_and_snapshots.md)、[Range Deletion Handling](../read_flow/08_range_deletions.md)、[Merge Operator Resolution](../read_flow/09_merge_resolution.md) 和 [Tombstone Lifecycle](../write_flow/08_tombstone_lifecycle.md)。写入节流边界见 [Flow Control and Write Stalls](../write_flow/07_flow_control.md)。

## Scope

本入口和 numbered chapters 是当前 checkout 的 source-backed navigation，不承诺未由源码或测试锚点证明的 tuning value。本文不复制章节正文，也不把 test name 当作本次运行结果。
