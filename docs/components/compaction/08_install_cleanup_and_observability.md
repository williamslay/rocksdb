# Compaction 安装、清理与可观测性

本章解释 verified output 离开 compaction job 后，如何成为一个可见的
Version，以及失败、暂停、恢复和 obsolete purge 如何处理文件。输入读取见
[04](04_input_io_and_memory.md)，键归约见 [05](05_key_reduction_semantics.md)，
local output 与 remote/resume 交接分别见 [06](06_subcompactions_and_outputs.md)
和 [07](07_remote_and_resumable.md)。本章不重复这些章节的 body；只描述安装层
如何消费它们交出的 metadata、paths 和 status。

## 安装 owner 与状态转换

安装 owner 是 `CompactionJob::Install`，不是 worker，也不是
`CompactionJob::Run`。`Install` 要求 DB mutex 已持有，先把 job 的
`internal_stats_` 合并到 CF 统计，再调用 `InstallCompactionResults`；安装失败
也会回写 `compact_->status`，使最终 state cleanup 能释放未安装 output 的 cache
条目（`db/compaction/compaction_job.cc:1184-1203,1349-1359`）。

| install state | owner/action | 可见性与失败语义 | source anchor |
| --- | --- | --- | --- |
| verified output | `CompactionJob::Run` 将已完成 metadata 留在 `CompactionState` | 仍不可被读路径当作 live Version；只交给安装 owner | `db/compaction/compaction_job.cc:Run`; [06](06_subcompactions_and_outputs.md#finish-sync-verify-retain-handoff) |
| edit assembled | `InstallCompactionResults` 添加 input deletions、SST/blob additions、garbage 和 cursor | edit 是候选版本，不代表已提交 | `db/compaction/compaction_job.cc:2336-2423` |
| manifest queued | `VersionSet::LogAndApply` 把 writer 放入串行队列；前一 writer 完成前等待 | 仍无新 Version 发布 | `db/version_set.cc:6778-6874` |
| manifest durable | `ProcessManifestWrites` 编码 edit，写 MANIFEST 并 `SyncManifest` | MANIFEST 写/sync 失败返回 status；quarantine list 保护待提交文件 | `db/version_set.cc:6471-6519` |
| CURRENT switched | 新 MANIFEST 时 `SetCurrentFile` 成功后才继续 | CURRENT 失败保留旧 manifest，并把相关文件放入 quarantine | `db/version_set.cc:6522-6539` |
| Version installed | status OK 后安装新 `Version`，并释放 compaction input ownership | 新 output 从此由 Version 引用；完成通知可在此之后观察 | `db/version_set.cc:6595-6605`; `db/compaction/compaction.cc:774-777` |
| failed install | 任一 pre-callback、manifest、sync、CURRENT 或 apply status 非 OK | 不宣称 output 可见；status 向上返回，未安装 output 进入 cleanup/quarantine 路径 | `db/version_set.cc:6855-6874,6515-6539`; `db/compaction/compaction_job.cc:1349-1359` |

“写入文件”与“版本可见”是两件事：output file 可以已经完成、sync 甚至通过
验证，但在 MANIFEST edit durable 且 Version apply 成功前，读路径仍不应引用它。
反过来，MANIFEST 失败也不表示可以立即无条件删除文件，因为提交失败保护和
pending-output 保护必须先阻止 purge 误删。

## VersionEdit 组装

`InstallCompactionResults` 先取 compaction 持有的 `VersionEdit`，调用
`Compaction::AddInputDeletions` 为每个 input level 写入 `DeleteFile`。随后遍历
每个 `SubcompactionState`：`AddOutputsEdit` 先添加 proximal level files，再添加
普通 output level files；每个 `VersionEdit::AddFile` 同时把 file number 放进
`files_to_quarantine_`，并按最大 output sequence 更新 `last_sequence_`
（`db/compaction/subcompaction_state.h:172-180`; `db/version_edit.h:802-840`）。

blob 不应伪装成 SST metadata。当前 subcompaction 的
`GetBlobFileAdditions()` 逐项进入 `VersionEdit::AddBlobFile`；该方法也把 blob
number 放入 quarantine。`BlobGarbageMeter` 的 inflow/outflow 差额按 blob file
聚合后，以 `AddBlobFileGarbage` 写入同一个 edit（`db/compaction/compaction_job.cc:2374-2405`;
`db/version_edit.h:862-900`）。这使 SST additions、blob additions 和已有 blob
的 garbage accounting 同属一次 manifest transition。

Round-robin compaction 在满足 reason 和 priority 条件时，把
`GetNextCompactCursor()` 结果作为 `AddCompactCursor` 写入 edit；它不是 output
file metadata，也不是 worker 的任意 checkpoint（`db/compaction/compaction_job.cc:2407-2417`;
`db/version_edit.h:844-860`）。恢复 progress 的 internal-key cursor 则是 [07](07_remote_and_resumable.md#4-可恢复-progress)
描述的另一条路径，不应混为 compaction picker cursor。

## 原子可见性与提交失败

`LogAndApply` 要求 mutex held 且不允许其他线程并发调用。它为 writer 排队，只有
当前 writer 成为队首后才执行 `pre_cb`；pre-callback 失败会从队列移除 writer、
唤醒下一个 writer 并返回（`db/version_set.h:1282-1301`; `db/version_set.cc:6822-6874`）。
在 `ProcessManifestWrites` 中，多个 edit 先应用到临时 `VersionBuilder`，再编码
写入 MANIFEST；原子 group 的 `remaining_entries_` 还会被校验。MANIFEST record
或 sync 失败时，不进入新 Version 安装。

新 descriptor 情况下，先创建并写新 MANIFEST，再通过 `SetCurrentFile` 切换
CURRENT；切换失败时旧 manifest 仍需可用，因此旧 manifest 和待提交文件进入
quarantine。提交成功后，旧 manifest 才追加到 `obsolete_manifests_`，由后续
`PurgeObsoleteFiles` 处理（`db/version_set.cc:6595-6603`）。这就是“失败不发布、
成功才切换”的边界；本章不把普通文件系统 rename 误写成整个 compaction 的
事务协议。

## 四类 cleanup

四类清理必须按 owner 和 retention 分开。`CleanupCompaction` 是 state/cache
cleanup，不等于物理删除；`CleanupAbortedSubcompactions` 才遍历所有曾创建的
SST/blob paths 并删除文件。shutdown 的取消与收尾是独立调用路径，不在普通
abort/pause 触发项中归类。

| cleanup class | 触发 | 文件动作 | status/retention |
| --- | --- | --- | --- |
| 成功安装 | `LogAndApply` 成功 | input 由新 Version 替代；旧文件等待 obsolete purge；output 由 Version 持有 | `OnCompactionCompleted` 可观察成功 commit |
| 普通 abort/pause | manual pause 或 compaction abort | `GetOutputFilePaths()` 覆盖已完成、partial 以及已从 outputs vector 移除的路径；逐个删除 SST/blob，再 `CleanupOutputs` | subcompaction 标为 `Incomplete(kCompactionAborted)`；记录 `COMPACTION_ABORTED`（`db/compaction/compaction_job.cc:772-833`） |
| install/generic failure | output verification、pre-callback、MANIFEST 或 apply 失败 | 不将 output 视为 live；`CleanupCompaction` 释放 builders/state，失败 status 触发 output cache 的 `ReleaseObsolete`；物理删除受 quarantine/pending 规则约束 | 不能把 generic failure 宣称为 resumable retention；`CleanupCompaction` 本身不保证删除 remote temporary files |
| resumable abort | `allow_resumption` 路径暂停 | 保留已完成 output 和 progress；当前未完成文件下次重做；不能套用普通 abort 的全删 | [07](07_remote_and_resumable.md#4-可恢复-progress) 的 progress 合同决定可恢复范围 |

普通 abort 的删除计数按路径扩展名区分 SST/blob，删除错误保留首个错误并记录
日志，但 `NotFound` 不当作新的删除失败。resumable retention 只由保存的
processed output records、internal-key resume cursor 和 output metadata 支持；
没有这些 records 时恢复 helper 返回 `NotFound`，不是“从任意 partial file 接着写”。

## Pending output、quarantine 与 obsolete purge

安装失败和后台 purge 之间存在保护层。`VersionEdit::AddFile`/
`AddBlobFile` 将新 number 放进 `files_to_quarantine_`；manifest commit 失败时
`ProcessManifestWrites` 将这批 numbers 提交给 error handler。与此同时，
`FindObsoleteFiles` 计算的 `min_pending_output` 防止尚在生成或等待提交的高号
SST/blob 被当作 obsolete。`PurgeObsoleteFiles` 对 table/blob 分别要求 live set、
pending number、blob keep boundary 或 active direct-write 状态满足，才允许删除
（`db/db_impl/db_impl_files.cc:522-705`）。

| 文件类别 | live/pending 判断 | 删除 owner | 观测点 |
| --- | --- | --- | --- |
| SST | `sst_live_set` 或 `number >= min_pending_output` 则保留 | `PurgeObsoleteFiles` -> `DeleteObsoleteFileImpl` | table deletion listener |
| blob | `blob_live_set`、pending、`min_blob_file_number_to_keep`、active direct-write 或 blob-specific keep policy 则保留 | 同上 | blob deletion listener |
| temp | SST/blob live set、pending manifest 或 OPTIONS 条件保护 | purge 分类逻辑 | 不应把 temp 当已安装 SST |
| manifest | 当前及更新 descriptor 保留；旧 manifest 成为 obsolete candidate | `obsolete_manifests_` -> purge | manifest write/CURRENT status |

物理删除前会按 filename/path 去重；不拥有文件的 secondary 不负责删除。删除
成功、文件已不存在、删除失败分别写不同级别日志。table/blob 删除随后调用
`EventHelpers::LogAndNotifyTableFileDeletion` 或
`LogAndNotifyBlobFileDeletion`（`db/db_impl/db_impl_files.cc:468-516,582-705`）。

## Pause、abort 与 resume 边界

入口 pause/abort 必须沿 status 传播，不能以空 output result 伪装成功。普通路径
清掉所有 tracked paths；可恢复路径只保留完成文件和 progress，当前 builder
对应的 partial file 重做。`CompactionJob::CleanupCompaction` 最后调用每个
subcompaction 的 `Cleanup` 并释放 job state（`db/compaction/compaction_job.cc:2624-2630`）。
remote/service 的 `OnInstallation` 只表示 service 选择的 staging-output adoption，
不表示 primary MANIFEST 已安装；其 timing 和 `kUseLocal`/`kFailure`/`kSuccess`
边界见 [07](07_remote_and_resumable.md#3-输入结果和安装交接)。

## Listener、reason、stats 与 IO counters

安装和观测不是同一个 callback。DB mutex held 时，`NotifyOnCompactionBegin` 构造
`CompactionJobInfo` 后释放 mutex 调用 listeners；pre-commit 只在 begin 已发生时
触发，并通过 compaction state 保证至多一次；completed 读取当前 L0 数量并在
回调后重新加锁（`db/db_impl/db_impl_compaction_flush.cc:1976-2079`）。因此 listener
不得假设 callback 在 mutex 内执行，也不得用 Completed 替代 PreCommit 做并发
文件 ownership bookkeeping。

`CompactionJobInfo` 暴露 CF、job id、input/output files、levels、reason、status、
table properties 和 stats；`CompactionReason` 是枚举值，不是自由文本。reason
计数进入 per-level `CompactionStats::counts[]`，同时保留 `kLevelMaxLevelSize`、
`kManualCompaction`、`kForcedBlobGC` 等触发原因（`db/internal_stats.h:159-227`;
`include/rocksdb/listener.h:433-500`）。

| observation | owner/字段 | 能回答什么 | 不应推出什么 |
| --- | --- | --- | --- |
| completion event | `compaction_finished` event log | duration、output level/files/bytes、records、compression、LSM state | event log 不证明 MANIFEST 以外的 remote staging 已删除 |
| reason | `CompactionReason` 与 `counts[]` | 为什么调度/统计这次 compaction | reason 不等于 picker 的全部内部分支 |
| compaction stats | `bytes_read_*`、`bytes_written*`、`bytes_moved`、input/output/dropped records | read/write amplification、吞吐、blob IO、filtered input | `bytes_moved` 不等于 bytes rewritten |
| IO counters | `COMPACT_READ_BYTES`、`COMPACT_WRITE_BYTES` 与 `file_*_nanos` | compaction read/write 量和 write/range-sync/fsync/prepare-write 时间 | 未开启 `measure_io_stats_` 时不能声称 file nanos 完整 |
| file deletion callbacks | table/blob deletion EventHelpers | obsolete deletion status、job、file number/path | deletion callback 不等于 compaction commit callback |

`CompactionJob::RecordCompactionIOStats` 记录 `COMPACT_READ_BYTES` 和对应 IO
计数；安装完成日志还输出 `file_write_nanos`、`file_range_sync_nanos`、
`file_fsync_nanos`、`file_prepare_write_nanos`，但只在 `measure_io_stats_` 开启时
加入 event log（`db/compaction/compaction_job.cc:1289-1319,2437-2439`）。
`CompactionStats` 区分非 output level、output level、blob read、table/blob write、
pre-compression write 与 moved bytes；安装时通过 `AddCompactionStats` 汇总到
CF 和 thread-priority 维度（`db/internal_stats.h:569-581`）。

## 观察与排错表

| observation | 先查 | 若失败，结论边界 |
| --- | --- | --- |
| output file 存在但读不到 | `InstallCompactionResults` -> `LogAndApply` status，再查 MANIFEST/CURRENT | 文件存在不代表 Version live；先查 commit，不要先删文件 |
| abort 后残留 SST/blob | `GetOutputFilePaths`、删除日志和 first deletion error | 可能是删除失败或 non-owned files；不能据此说 output 已安装 |
| purge 没删 obsolete | `sst_live_set`/`blob_live_set`、`min_pending_output`、quarantine | pending/quarantine 是保护，不是泄漏证据；再查 owner 与 deletion status |
| resume 重复或跳过 | persisted internal-key cursor、processed output records、table properties | partial current file 必须重做；无合法 progress 不能宣称 resume |
| listener 顺序错误 | Begin、PreCommit、Completed 的 job id/input file set | PreCommit 是 commit 前并发窗口；Completed 不能替代它 |
| amplification/IO 数字异常 | output-level vs non-output-level 字段、blob 字段、`measure_io_stats_` | 不把 bytes moved、blob bytes、file nanos 混成一个 WAF 数字 |

## Source-backed scope receipt

本章覆盖 current source 中的 local install path、MANIFEST atomic visibility、
SST/blob additions、cursor、pending/quarantine、四类 cleanup、resumable retention、
obsolete purge、pause/abort、listener callbacks、reason、stats 和 IO counters。
没有执行 build、test、benchmark、stress 或 Jekyll；因此本文没有 runtime receipt，
只提供结构性 source receipts。remote service 的具体 temporary-file deletion
仍属于 service contract，不在此章扩大为通用保证。
