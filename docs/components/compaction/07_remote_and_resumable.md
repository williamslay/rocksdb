# Remote 与可恢复 compaction

本章只解释 compaction service、secondary 上的 `OpenAndCompact`，以及可恢复执行的边界。普通本地输出和 MANIFEST 安装由 [06_subcompactions_and_outputs.md](06_subcompactions_and_outputs.md) 的输出章节、后续安装章节负责；这里说明它们如何交接。

## 1. 三种执行器

| 维度 | local executor | remote executor | resume executor |
| --- | --- | --- | --- |
| executor | `CompactionJob::Run` 在 primary DB | `CompactionServiceCompactionJob::Run` 在 service worker | `CompactionJob` 读取 progress 后继续 |
| subcompaction count | 可由 picker 分割为多个 | 强制一个，`Run` 对 `sub_compact_states.size() == 1` 做断言 | `OpenAndCompact` 当前只支持一个 subcompaction；该限制不是“恢复每个已有 state” |
| validation | 本地 `CompactionJob` pipeline 的 output verification | 处理 key/value、flush、目录 fsync 并返回 metadata；remote service **不执行完整 local `CompactionJob::Run` verification pipeline** | 恢复时读取 output table properties，并检查 seek、iterator、metadata |
| metadata ownership | primary job 构建并安装 | service 产生 `CompactionServiceResult::output_files`，primary 采用结果 | progress 保存 `FileMetaData` 和 counters，恢复端重建 output state |
| retention | 普通 abort 清理未安装输出 | worker output path 的删除不由通用 job 保证；具体 service 可在安装后的 optional callback 中处理 | resumable abort 保留已完成输出和 progress，当前文件重做 |
| fallback | 不适用 | `kUseLocal` 回到 local processing | resume 无有效状态时按调用选项从头开始 |
| install owner | primary `VersionSet::LogAndApply` | primary，不是 remote service | primary，恢复输出仍需最终 local install |

这张表的“验证”不是同义词：remote job 使用 `ProcessKeyValueCompaction`，但不应写成 remote 直接复刻本地完整验证流程。service 也不直接写 primary 的 MANIFEST。

## 2. Schedule、Wait 与 status

公共接口在 `include/rocksdb/options.h`，不是不存在的 `include/rocksdb/compaction_service.h`。`CompactionServiceJobInfo` 携带 `db_name`、`db_id`、`db_session_id`、CF、job、priority、reason、输入级别和 output level。`CompactionService::Schedule` 收到序列化 input，返回 `CompactionServiceScheduleResponse`；随后 primary 用 scheduled job id 调 `Wait`。service 以 `CompactionServiceJobStatus` 映射协议结果，典型状态是 `kSuccess`、`kFailure`、`kAborted`、`kUseLocal`。

`CompactionJob::ShouldUseLocalCompaction` 是 fallback 边界。service 成功时，subcompaction 结果被采用；service 返回 `kUseLocal` 时才继续本地处理，并把 `is_remote_compaction` 设回 false。失败不是自动等价于 fallback，具体 service 可以决定返回 `kUseLocal` 还是 `kFailure`。`db/compaction/compaction_service_test.cc` 的 fake service 覆盖 Schedule、Wait、result override 和 `OnInstallation`；`db_stress_tool/db_stress_compaction_service.{h,cc}` 展示取消、失败 fallback 和轮询协议。

## 3. 输入、结果和安装交接

`CompactionServiceInput::Write` 在 `db/compaction/compaction_service_job.cc` 写 binary format marker，再由 `OptionTypeInfo::SerializeType` 序列化。输入包含 input files、snapshot、output level，以及可选 `begin`/`end`。`CompactionServiceCompactionJob::Prepare` 把它们变成一个 key range，调用基类 `CompactionJob::Prepare`。

`CompactionServiceResult::Read` 先拒绝过短数据，再检查 format version，解析时允许未知字段。`CompactionServiceCompactionJob::Run` 填充 status、`internal_stats`、output level、output path、以及每个 output 的 file number、size、sequence、smallest/largest key、timestamps、checksum、table properties、temperature 和 proximal 标记。status 非 OK 时不发布 output file metadata。

primary 侧 `DBImplSecondary::CompactWithoutInstallation` 依据输入 file numbers 重建 `Compaction`，建立 output directory，调用 service job，并在该方法中执行 `result->status = s`。因此，结果 status 的赋值 owner 是 `DBImplSecondary::CompactWithoutInstallation`，不是 service worker 或 `DB::OpenAndCompact`。这一步只产出可供 primary 采用的文件和 metadata。`OnInstallation` 是 service 选择实现的 staging-output adoption notification，不表示 primary 的 MANIFEST 安装已完成：remote job 在解析 result 后将 staging files rename 到 primary 的 compaction output state、或在采用前触发 `kUseLocal`/`kFailure` 分支时调用它；成功通知也发生在最终 local install 之前。最终可见性仍由本地安装路径完成。

## 4. 可恢复 progress

`OpenAndCompactOptions` 在 `include/rocksdb/options.h` 提供 `allow_resumption` 和 `canceled`。`allow_resumption` 是 experimental 控制：开启时从 `output_directory` 读取并按完成 output file 顺序持久化 progress；关闭时要求 output directory 调用前为空。progress writer 在完成新 output file 后 sequential、best effort 地写入 `VersionEdit`，不是每个 key 都落盘。

`CompactionProgress` 中的 `SubcompactionProgress` 记录 `next_internal_key_to_compact`、processed input records，以及 output/proximal output 两组 `SubcompactionProgressPerLevel`。每组保留 `FileMetaData`、processed output records 和上次持久化 output 数量。`CompactionJob::UpdateSubcompactionProgress` 负责更新，`VersionEdit::SetSubcompactionProgress` 负责承载记录；`ldb` 的 compaction progress dump 可读这些 records。

恢复由 `CompactionJob::MaybeResumeSubcompactionProgressOnInputIterator` 完成。没有 processed output records 返回 `NotFound`。否则对 input iterator `Seek(next_internal_key_to_compact)`，invalid 或 iterator error 映射为 `Corruption`。恢复时读取每个 output 的 table properties，调用 `RestoreCompactionOutputs` 重建 output state，并推进 `VersionSet` file number，避免新文件号冲突。已保存 input/output counters 回填 job stats；若没有 per-key placement，则跳过 proximal progress。

恢复位置不是“最后一个 key 的模糊位置”，而是持久化 internal key。中断发生在当前 output file 尚未完成时，该 partial file 不算完成 output，恢复必须重做这段工作。已完成 output 可保留并继续追加。恢复后的 metadata 和 counters 属于恢复端重建的执行状态，最终仍须由 primary 安装。

## 5. 限制、取消与清理

`DBImplSecondary::CompactWithoutInstallation` 在入口检查 `options.canceled`，取消返回 `Incomplete(kManualCompactionPaused)`。service worker 的 `Wait` 也可返回 `kAborted`；db_stress fake 在取消标志后停止等待。取消、远端失败、resume state 损坏要通过 status 传回，不能用空 result 冒充成功。Malformed input 或不支持的 binary version 由 `CompactionServiceResult::Read` 返回 `InvalidArgument` 或 `NotSupported`。

`allow_resumption` 会因 `paranoid_file_checks` 或 `verify_output_flags` 含 `kVerifyIteration` 而被禁用，因为中断后 hash state 没有持久化。除此之外，`CompactionJob::UpdateSubcompactionProgress` 只在以下四个条件都满足时保存边界：

1. comparator 没有 user-defined timestamp；带 timestamp 的 compaction progress 不持久化。
2. 边界两侧不是 range-deletion sentinel；含 range deletion 的边界不持久化，因为 range deletion 可能跨文件边界。
3. 相邻 output table 的 user key 不相同（比较时忽略 timestamp）；共享同一 user key 的相邻文件边界不持久化。
4. 当前 key 没有已经被 input iterator lookahead 扫描但尚未输出；lookahead 边界不持久化，否则恢复 `Seek()` 会重复处理输入。

这些是 progress 持久化限制，不是对 compaction 语义的额外保证。当前实现还留下 snapshot checker、完整 `Compaction` 序列化、table property 读取优化等 TODO；尤其不要把当前 API TODO 当成公开 abort/resume 保证。

清理语义必须分开：

| 情形 | 输出处理 | progress/status |
| --- | --- | --- |
| `CleanupAbortedSubcompactions` | 物理删除各 subcompaction 跟踪的 SST/blob paths，并清理 output builders；这是 abort 的 physical-file cleanup | subcompaction status 标为 `Incomplete(kCompactionAborted)` |
| `CleanupCompaction` | 调每个 state 的 `Cleanup`，释放 job/state；它是 state cleanup，不负责物理删除 remote temporary files | job 对象被销毁 |
| resumable abort | resumable 路径保留可恢复的已完成 outputs 和 progress，未完成当前文件重做；不得套用普通 abort 的删除结论 | 保留到下一次 `OpenAndCompact` |
| generic failure | failure status 沿调用链返回；是否保留文件只能按上述 resumable 路径和具体 service 合同判断 | 不能把 generic failure 宣称为 resumable retention 或成功 |
| remote result adoption notification | service 选择的 `OnInstallation` 在 staging output 被采用到 compaction output state 后、最终 primary install 前通知；result parse fallback 用 `kUseLocal`，output import failure 用 `kFailure`，成功采用用 `kSuccess` | result/staging state 是否清理由具体 service 回调决定 |
| malformed/corrupt resume | 不采用损坏 metadata，按 options 的 fresh-compaction fallback 或返回 non-OK | 不能宣称恢复成功 |

因此，不能把所有 abort/failure 合并成同一种删除语义：`CleanupAbortedSubcompactions` 负责物理文件删除，`CleanupCompaction` 负责 state cleanup，resumable 路径负责保留可恢复状态，而 generic failure 只保证 status 传播。`CompactionServiceCompactionJob::CleanupCompaction` 当前委托 `CompactionJob::CleanupCompaction` 做 state cleanup；它不等于删除 remote temporary files。remote output deletion 只有在具体 service 实现选择执行时，才由 optional `OnInstallation` 回调负责。

## 6. 使用边界与证据入口

`OpenAndCompactOptions.allow_resumption`、`OpenAndCompactOptions.canceled` 以及 `tools/db_bench_tool.cc` 中的 `OpenAndCompact`、`compact_files_one_in`、`compact_range_one_in`、`compact_range_width` flag 名称仅作为 source references。本文不运行 benchmark，也不把 flag 名称写成稳定 public resume API。

建议按以下顺序读代码：`include/rocksdb/options.h:CompactionService`，`db/compaction/compaction_service_job.cc:CompactionJob::ProcessKeyValueCompactionWithCompactionService`，`db/compaction/compaction_service_job.cc:CompactionServiceCompactionJob::Run`，`db/db_impl/db_impl_secondary.cc:DBImplSecondary::CompactWithoutInstallation`，`db/compaction/compaction_job.cc:CompactionJob::ShouldUseLocalCompaction`、`CompactionJob::MaybeAssignCompactionProgressAndWriter` 和 `CompactionJob::MaybeResumeSubcompactionProgressOnInputIterator`，最后看 `db/compaction/compaction_service_test.cc`。这些测试和 tool 是行为证据，不是新的协议承诺。
