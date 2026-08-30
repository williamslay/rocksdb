# Local Subcompactions and Outputs

本章只描述 local `CompactionJob` 如何把一次 compaction 变成若干有序的
`SubcompactionState`，以及如何把 key/value 变成可验证的 SST 和 blob 输出。
`VersionSet::LogAndApply`、MANIFEST installation 属于 chapter 08；remote executor
的差异属于 chapter 07。输入读取和 key reduction 分别见
[chapter 04](04_input_io_and_memory.md) 与 [chapter 05](05_key_reduction_semantics.md)。

## Local execution order

| 顺序 | 状态或动作 | canonical owner | 结果与不变量 |
| --- | --- | --- | --- |
| 1 | 创建 job 状态 | `CompactionJob::CompactionJob` in `db/compaction/compaction_job.cc` | 构造函数创建本次 job 的 `CompactionState`，保存 compaction 和 job 资源，不安装版本 |
| 2 | 准备 boundaries | `CompactionJob::Prepare` in `db/compaction/compaction_job.cc` | 未 supplied single range 且 compaction 要形成 subcompactions 时先生成 boundaries；supplied single-range path 绕过生成 |
| 3 | 初始化 subcompaction 状态 | `CompactionJob::Prepare` | 生成路径按有序 boundaries 建立独立 range、status、IO status、统计和 `CompactionOutputs`；single-range path 建立一个 supplied range |
| 4 | 启动 workers | `CompactionJob::RunSubcompactions` | worker 0 在当前线程运行，其余 worker 各在线程运行，全部 `join` 后才清理空输出 |
| 5 | 建立 clipped input | `CreateInputIterator`、`SubcompactionKeyBoundaries`、`ClippingIterator` | `[start,end)` 语义，`end` exclusive；迭代器不得越过本子范围 |
| 6 | reduction 与 key processing | `CompactionJob::ProcessKeyValueCompaction`、`ProcessKeyValue` | merge/filter/tombstone 等语义完成后，按顺序交给 output builder；错误写入 subcompaction status |
| 7 | 打开并持续写 output | `OpenCompactionOutputFile`、`CompactionOutputs::NewBuilder`、`TableBuilder` | file number、path、temperature、compression、metadata 和 builder 绑定 |
| 8 | 结束当前文件或 split | `CompactionOutputs`、`SstPartitioner`、builder finish path | builder `Finish` 后 metadata 成为候选输出；`WriterSyncClose` 随后 capture checksum，不能再向该文件写入 |
| 9 | 完成 subcompaction | `FinalizeSubcompaction`、`RemoveEmptyOutputs` | 空文件移除，非空输出保留在 state；status 与 IO status 向 job 汇总 |
| 10 | job 级同步与验证 | `CompactionJob::Run`、`SyncOutputDirectories`、`VerifyOutputFiles` | 先收集错误，再 sync，后重读验证；验证失败不会进入 installation |
| 11 | 交接安装层 | `FinalizeCompactionRun` is `void`; `CompactionJob::Run` returns `Status`，输出仍由 `CompactionState` 携带 | `Run` 这里只交出 verified metadata 和 paths；MANIFEST apply 由 chapter 08 解释 |

`CompactionServiceCompactionJob::Run` 是 remote 的相邻路径，不应被写成完整 local
`CompactionJob::Run` verification。它执行一个 subcompaction 并构造服务结果，remote
差异见 chapter 07。

## Range clipping and threading

`CompactionJob::CompactionJob` 在 `db/compaction/compaction_job.cc` 的成员初始化列表中
创建 `CompactionState`。`Prepare` 先检查调用者是否 supplied single range。没有 supplied
single range 且 compaction 需要 subcompactions 时，才由 `GenSubcompactionBoundaries` 根据
输入和目标并行度生成 boundaries；supplied single-range path 绕过生成。随后按选定路径
构造 `SubcompactionState`。
`RunSubcompactions` 为下标 1 到末尾创建线程，当前线程处理下标 0，最后逐个
`join`。因此输出汇合不是 worker 之间共享 builder，而是每个
`SubcompactionState` 独立累积后再聚合。

`ProcessKeyValueCompaction` 为单个 state 创建 `SubcompactionKeyBoundaries`，再创建
input iterator 和 compaction iterator。`ClippingIterator` 位于
`db/compaction/clipping_iterator.h`，在 `Seek`、`SeekForPrev`、`Next`、`Prev`
后强制 lower/upper bound。upper bound exclusive，等于 `end` 的 key 必须不可见。
range deletion 不能简单按 point key 截断，`CompactionOutputs::AddRangeDels` 会按
当前 output 的 lower/upper bound 裁剪 tombstone，并在 subcompaction 边界处延伸覆盖，
同时保持 internal-key 顺序不重叠。

## State and output ownership

`CompactionState` (`db/compaction/compaction_state.h/.cc`) 是 job 生命周期容器，保存
compaction、所有 `SubcompactionState`、job status、统计和资源。单个
`SubcompactionState` (`db/compaction/subcompaction_state.h/.cc`) 保存 `[start,end)`、
本地 `status`/`io_status`、range-del aggregator、job info，以及两类
`CompactionOutputs`。后者 (`db/compaction/compaction_outputs.h/.cc`) 保存 output
metadata、当前 `WritableFileWriter`、`TableBuilder`、validator 和 output paths。

`GetMutableCompactionOutputs` 与 `GetMutableProximalOutputs` 区分普通 output level
和 proximal level。tiered 不是 picker style，而是现有路径中的 proximal/per-key
placement 行为，输出类别由 compaction 状态和 key processing 决定。

## Output boundary decisions

| 决策点 | canonical symbol/path | 触发 | 边界含义 |
| --- | --- | --- | --- |
| file size | `CompactionOutputs::ShouldStopBefore` in `db/compaction/compaction_outputs.cc` | 当前 builder 达到 `max_output_file_size` 语义 | 结束当前 SST，再以新 metadata/builder 写后续 key |
| grandparent overlap | `CompactionOutputs::ShouldStopBefore` | 新 key 会使 grandparent overlap 超出规则 | 避免输出文件与 grandparents 产生不受控 overlap |
| range tombstone | `CompactionOutputs::AddRangeDels` | 已决定的 output 需要 tombstone 元数据调整 | split 后裁剪并延长 file key range，保持覆盖和 non-overlap，不独立触发 split |
| explicit partition | `SstPartitioner` in `db/compaction/sst_partitioner.cc` | non-L0 `CompactionOutputs` 构造时创建 partitioner，且 partitioner 判断 key boundary | non-L0 按 partition policy split，不改变 key reduction 结果；L0 constructor 将 partitioner 置为 `nullptr` |
| subcompaction boundary | `GenSubcompactionBoundaries` | worker range 划分 | worker 间 user-key 范围不重叠，边界 key 归属明确 |
| proximal placement | `CompactionOutputs::IsProximalLevel`, `Compaction::SupportsPerKeyPlacement`, and output routing | per-key/proximal policy | 写入对应 output collection，不把 tiered 当 picker |

size、grandparent、partitioner 和 range boundary 是不同原因。L0 output constructor 将
partitioner 置空，因此 L0 不使用显式 partitioner，也不使用这些 partition split rules；L0
绕过 ordinary size 和 grandparent splitting。先决定是否结束
当前 output，再完成 builder，最后打开新文件。不能用 output path、compression 或
temperature 反推 split 原因。

## Builder details and file metadata

`OpenCompactionOutputFile` (`db/compaction/compaction_job.cc`) 分配 file number，生成
SST name，调用 `GetOutputTemperature` 写入 `FileOptions` 和 `FileMetaData`，并把
output path 注册进 `CompactionOutputs`，以便 abort cleanup 找到未安装文件。它创建
`WritableFileWriter` 和 `TableBuilderOptions`，其中包含
`output_compression()`、compression options、output level 和 output file size，随后
调用 `outputs.NewBuilder`。

compression 是 builder 的写格式选择，path 是 file system 位置，temperature 是
文件系统写属性，三者职责不同。`CreateBlobFileBuilder` (`compaction_job.cc`) 为
需要独立 blob 文件的 value 建立 blob builder；blob metadata 和 garbage accounting
仍须随 subcompaction status 一起交给后续安装层，不能把 blob 当普通 SST block。

table properties 在 output 完成后由 builder 产生，并在 job 级
`SetOutputTableProperties` 汇总。启用 `paranoid_file_checks` 或
`VerifyOutputFlags::kVerifyIteration` 时，output validator/hash 支持后续重读比较。

## Finish, sync, verify, retain, handoff

输出生命周期必须按以下顺序理解：

1. builder 接收有序 records，遇到 boundary 时完成当前 SST。
2. builder `Finish` 写入 footer 和 table properties，随后 output metadata 标记为候选完成；checksum
   capture 不属于 builder `Finish`，而在之后的 `WriterSyncClose` 完成 sync/close 后从
   `WritableFileWriter` 读取并写入 metadata。
3. `FinalizeSubcompaction` 汇总 builder、blob builder、迭代器 status 与 IO status。
4. `CompactionJob::Run` 收集所有 subcompaction errors。abort 或 manual pause 时，
   `CleanupAbortedSubcompactions` 删除普通 local partial outputs；有
   `compaction_progress_writer_` 的 resumable job 保留恢复所需 output。
5. 只有 status 仍为 OK 时，`SyncOutputDirectories` 才同步 output directories。
6. sync 成功后，`VerifyOutputFiles` 重读并检查 output；失败 status 继续向上返回。
7. verified output、table properties、stats 和 paths retained 在 `CompactionState`，交给
   installation owner。`CompactionJob::Run` 不调用 `VersionSet::LogAndApply`；此范围不把
   该 no-install claim 扩展到其他 compaction/service entry points，也不声称 remote
   service 完成同样的 local verification。

## Failure invariants

- 任一 worker 的 `status` 或 `io_status` 失败，job 不得把 output 当成功结果交接。
- `RunSubcompactions` 必须等待所有 worker，避免尚未关闭的 file 被 sync 或验证。
- 空 output 不产生可安装 SST，`RemoveEmptyOutputs` 在 worker join 后运行。
- output verification 发生在 directory sync 之后，verification 失败仍需沿 status 路径传播。
- ordinary abort 清理未安装文件，resumable abort 为 resume 保留 partial outputs；详细
  cleanup 分类由 chapter 08 负责。
- input range、output range、range tombstone coverage 都必须保持 internal comparator
  的顺序不变量。

## Source anchors

- `db/compaction/compaction_job.cc`: `CompactionJob::Prepare`, `GenSubcompactionBoundaries`, `RunSubcompactions`, `ProcessKeyValueCompaction`, `OpenCompactionOutputFile`, `CreateBlobFileBuilder`, `SyncOutputDirectories`, `VerifyOutputFiles`, `Run`。
- `db/compaction/compaction_state.h` and `compaction_state.cc`: `CompactionState` lifecycle container。
- `db/compaction/subcompaction_state.h` and `subcompaction_state.cc`: `SubcompactionState`, `CleanupOutputs`, `AddOutputsEdit`。
- `db/compaction/compaction_outputs.h` and `compaction_outputs.cc`: `CompactionOutputs`, `ShouldStopBefore`, `AddRangeDels`, builder and validator ownership。
- `db/compaction/clipping_iterator.h`: `ClippingIterator` bound enforcement。
- `db/compaction/sst_partitioner.cc`: `SstPartitioner` output partition decisions。
- `db/builder.h` and `db/builder.cc`: `TableBuilder` creation and finish contract。
- `db/compaction/compaction_job_test.cc`: local output, boundary, status and cleanup evidence。
