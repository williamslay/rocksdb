# Compaction 入口与调度

本章只解释 compaction 如何进入调度系统、如何获得后台执行机会，以及手动任务如何与自动任务冲突、暂停、取消和收尾。输入选择算法不在本章展开，键归约语义也不在本章展开。通用写入节流和 write stall 机制见 [write flow 07](../write_flow/07_flow_control.md)。

## 先看调用边界

自动路径通常从 `ColumnFamilyData::NeedsCompaction()` 形成待处理状态。`DBImpl::EnqueuePendingCompaction()` 在 `db/db_impl/db_impl_compaction_flush.cc` 的 `mutex_` 保护下，检查 `reject_new_background_jobs`、`queued_for_compaction` 和 `NeedsCompaction()`，再通过 `AddToCompactionQueue()` 保持 `ColumnFamilyData` 引用并增加 `unscheduled_compactions_`。真正投递线程由 `MaybeScheduleFlushOrCompaction()` 负责。

`MaybeScheduleFlushOrCompaction()` 是调度总闸门，调用者必须持有 `mutex_`。数据库未成功打开、read only、后台工作暂停、硬错误且不在恢复、shutdown、compaction 暂停或全局 abort 时，它直接返回。存在 exclusive manual compaction 时，只允许手动任务，自动 compaction 不会被投递。通过检查后，函数按 `GetBGJobLimits()` 限制投递后台任务，默认 compaction 任务使用 `Env::Priority::LOW`，而 bottom priority 任务由相应的后台计数和队列路径管理。

## 入口矩阵

| entry | trigger/API | scheduling owner | lock/conflict rule | retry/cancel outcome | next symbol | test/example anchor |
| --- | --- | --- | --- | --- | --- | --- |
| automatic | memtable flush、LSM 状态更新或 `NeedsCompaction()` | `DBImpl::EnqueuePendingCompaction()`、`AddToCompactionQueue()`、`MaybeScheduleFlushOrCompaction()` | `mutex_`；暂停、硬错误、shutdown、exclusive manual 会阻止自动投递 | 队列保留未调度工作；后台返回后重新调度 | `BGWorkCompaction()`、`BackgroundCallCompaction()`、`BackgroundCompaction()` | `db/db_test.cc:DBTest.AutomaticConflictsWithManualCompaction` |
| `CompactRange` | `DB::CompactRange()` API | `DBImpl::CompactRange()`、`CompactRangeInternal()`、`RunManualCompaction()` | 开始前检查 manual pause、abort 和 `CompactRangeOptions::canceled`；manual state 参与重叠检查 | 返回 `Incomplete` 子码，或分段完成后继续处理范围 | `RunManualCompaction()` | `db/db_test.cc:DBTest.AutomaticConflictsWithManualCompaction`、`db/db_compaction_test.cc:DBCompactionTest.ManualAutoRace` |
| `CompactFiles` | `DB::CompactFiles()` API，给定输入文件名 | `DBImpl::CompactFiles()`，同步调用 `CompactFilesImpl()` | 在 `mutex_` 下引用 `Version`；由显式输入和 manual pause/cancel 协调 | 状态原样返回；失败后强制发现 obsolete files 并清理 | `CompactFilesImpl()` | `db/compact_files_test.cc`、`examples/compact_files_example.cc` |
| periodic | `DBImpl::TriggerPeriodicCompaction()` 的周期任务 | 周期触发器重算 score、`EnqueuePendingCompaction()`，再调用 `MaybeScheduleFlushOrCompaction()` | 跳过 dropped 或已排队的 CF；仅考虑时间型或 read triggered 配置 | 没有可排队 CF 时不产生任务；后台结果遵循普通错误和取消路径 | `TriggerPeriodicCompaction()` | `db/db_impl/db_impl.cc:DBImpl::TriggerPeriodicCompaction`、`db/periodic_task_scheduler_test.cc:TriggerCompactionTest.QueuesAllTimeBasedOptions` |

矩阵中的 `PickCompaction()` 只作为下一个调度边界。具体 score、candidate、overlap 和 picker 算法由后续章节负责，本章不把任何 picker 分支当作入口规则。

## 自动调度与队列优先级

`MaybeScheduleFlushOrCompaction()` 先处理 flush。若 HIGH priority flush pool 存在，flush 使用 `Env::Priority::HIGH`；若 HIGH pool 为空，flush 可暂时投递到 LOW pool，并把 flush 和 compaction 的已调度数量共同纳入 flush 上限。之后它检查 `bg_compaction_paused_`、`compaction_aborted_`、错误状态和 `HasExclusiveManualCompaction()`，最后在 `bg_job_limits.max_compactions` 内，把 `unscheduled_compactions_` 转换成 `CompactionArg`，调用 `env_->Schedule()` 投递 `BGWorkCompaction()`。

自动 worker 的实际调用链是 `BGWorkCompaction()` -> `BackgroundCallCompaction()` -> `BackgroundCompaction()` -> `PickCompactionFromQueue()`。`BGWorkCompaction()` 由线程池回调进入，`BackgroundCallCompaction()` 负责 job 计数和收尾，`BackgroundCompaction()` 在锁内处理状态并从队列取出可执行 CF。队列计数和 `ColumnFamilyData` 引用不是完成标志。只有 worker 收尾时递减相应计数并再次调用 `MaybeScheduleFlushOrCompaction()`，调度器才会观察到新产生的工作。

`RequestCompactionToken()` 是另一层并发限制。若 CF 没有 `compaction_thread_limiter`，它允许继续；有 limiter 时，只有拿到 `TaskLimiterToken` 才继续，`force` 决定 token 请求是否强制。它限制任务数量，不替代 DB 的暂停、冲突或 shutdown 规则。

## 后台执行、重试与清理

`BackgroundCallCompaction()` 在 `db/db_impl/db_impl_compaction_flush.cc` 中创建 `JobContext`，在锁内增加运行计数，并调用 `BackgroundCompaction()`。进入后台 job 前，它用 `CaptureCurrentFileNumberInPendingOutputs()` 记录当前文件号，保护尚未安装的输出文件不被 obsolete-file 处理误删。

返回状态决定等待策略：

1. `Busy` 唤醒等待者，释放锁后睡眠 10000 微秒，再继续收尾，避免热循环。
2. 环境错误若不是 shutdown、manual pause、CF dropped 或 compaction aborted，则记录后台错误计数，刷新日志，释放锁睡眠 1000000 微秒，再重试。
3. manual pause 只记录暂停日志，不把暂停误判为环境错误。
4. 无论成功或失败，先释放 pending-output 记录。失败且状态不是 `ShutdownInProgress`、`ManualCompactionPaused`、`CompactionAborted`、`ColumnFamilyDropped` 或 `Busy` 时，`FindObsoleteFiles()` 强制全扫描，以覆盖未进入 `JobContext` 的临时文件。
5. `PurgeObsoleteFiles()` 和 `JobContext::Clean()` 在锁外执行，随后递减运行和已调度计数。

收尾最后再次调用 `MaybeScheduleFlushOrCompaction()`，再调用 `NotifyOnBackgroundJobPressureChanged()`。若有进展、后台任务归零、仍有手动任务，或没有未调度 compaction，才向 `bg_cv_` 广播。`SignalAll()` 后不应继续访问 DB 状态，因为 shutdown 等待者可能立即销毁对象。

`BackgroundCompaction()` 在真正执行前还会经过后台工作停止、shutdown、错误状态、输入冲突、空间检查和 token 检查。`EnoughRoomForCompaction()` 将空间预留交给 `SstFileManagerImpl::EnoughRoomForCompaction()`，空间不足时记录 `COMPACTION_CANCELLED`。这属于资源压力反馈，不等于 picker 没有候选。

## 压力和写入反馈

调度状态通过 `NotifyOnBackgroundJobPressureChanged()` 采集 `BackgroundJobPressure`，回调 listener 时暂时释放 `mutex_`，回调结束后重新取得锁。`CaptureBackgroundJobPressure()` 暴露 scheduled/running counts、`compaction_speedup_active` 和 write-stall proximity；这些字段分别描述后台工作量、speedup 状态和接近写停顿阈值的程度。`made_progress` 只参与 `bg_cv_` signaling，不属于 listener pressure snapshot；queued count 和 error count 也不应归因于该 listener pressure state。

写入线程的节流策略属于 write flow 的 canonical owner，本章只保留 compaction 调度接口边界。`BackgroundCallCompaction()` 的 `made_progress` 会唤醒等待者，使 `DelayWrite` 等待条件重新评估；不要把此信号解释成固定吞吐量或通用参数建议。参见 [write flow 07](../write_flow/07_flow_control.md)。

## 周期触发

`DBImpl::TriggerPeriodicCompaction()` 在 `db/db_impl/db_impl.cc` 持有 `mutex_`，遍历未 dropped 的 CF。已排队 CF 被跳过。若 `GetMinTimeBasedCompactionInterval()` 大于零，或 `read_triggered_compaction_threshold` 大于零，函数重算 `ComputeCompactionScore()`，调用 `EnqueuePendingCompaction()`，最后调用 `MaybeScheduleFlushOrCompaction()` 并广播条件变量。

周期触发只负责重新计算 score 并把符合条件的 CF 重新放入调度队列，不保证每次都生成 compaction。具体 candidate 和 picker 规则留给后续章节 `02_scores_and_input_picking.md` 与 `03_compaction_styles.md`，本章只保留调度边界。周期原因在 `CompactionReason::kPeriodicCompaction` 中表达；测试锚点是 `db/periodic_task_scheduler_test.cc:TriggerCompactionTest.QueuesAllTimeBasedOptions`。

## `CompactRange` 与 manual worker

`DBImpl::CompactRange()` 先读取 `manual_compaction_paused_`、`compaction_aborted_` 和用户取消指针。timestamp comparator 存在时，它先通过 `MaybeAddTimestampsToRange()` 补齐范围，再进入 `CompactRangeInternal()`。后者建立 manual state 并调用 `RunManualCompaction()`，按范围推进，可能以 `incomplete` 表示本次只完成部分范围。

`ManualCompactionState` 位于 `db/db_impl/db_impl.h`。其中 `exclusive` 表示只允许一个 manual compaction 影响后台自动调度，`in_progress`、`done`、`incomplete` 和 `canceled` 分别表达生命周期状态。`ShouldntRunManualCompaction()` 对 exclusive 请求检查后台 compaction 计数，对非 exclusive 请求检查队列中位于其前方且范围重叠、尚未开始的 manual 请求。

`PauseBackgroundWork()` 增加 `bg_compaction_paused_`，等待所有 flush 和 compaction 已调度计数归零后增加 `bg_work_paused_`。`ResumeAllCompactions()` 递减 abort 计数，只有回到零时才重新调用 `MaybeScheduleFlushOrCompaction()`。`DisableManualCompaction()` 和 `EnableManualCompaction()` 作用于 manual 队列，不应与自动队列状态混为一谈。

## `CompactFiles` 与显式输出

`DBImpl::CompactFiles()` 校验 `ColumnFamilyHandle`，创建 job context，在 `mutex_` 下引用当前 `Version`，调用 `CompactFilesImpl()`，然后释放引用。`CompactFilesImpl()` 消费调用方给出的输入文件名和 output level/path 参数，执行显式 compaction 安排；它不是自动 picker 的入口，也不应被描述为自动选择输入。

`CompactFiles()` 返回前单独执行 `FindObsoleteFiles()`。失败时传入强制扫描条件，随后在锁外调用 `PurgeObsoleteFiles()` 和 `JobContext::Clean()`。因此显式 API 的失败清理与后台 worker 的清理逻辑相似，但调度 owner 不同。最小可读示例是 `examples/compact_files_example.cc`；行为锚点是 `db/compact_files_test.cc`。

## 停止、取消与 pending output

`DBImpl::CancelAllBackgroundWork()` 先取消 periodic task scheduler，再标记 `shutting_down_`，广播 `bg_cv_`，按 `wait` 决定是否等待后台工作。存在 compaction service 时，它还调用 `CancelAwaitingJobs()`。shutdown、manual pause、compaction abort 和用户 `canceled` 指针是不同状态，返回状态和清理动作必须分别阅读，不能统称为 retry。

后台 job 的 pending-output 保护由 `pending_outputs_` 列表，以及 `CaptureCurrentFileNumberInPendingOutputs()` 和 `ReleaseFileNumberFromPendingOutputs()` 成对维护。前者在 job 开始时登记当前文件号，后者在 `BackgroundCallCompaction()` 收尾时移除登记。该列表保护临时输出生命周期，不表示文件已经进入 MANIFEST，也不替代 `VersionSet::LogAndApply()` 的安装语义。

## 读源码顺序

建议按以下顺序核对入口：

1. `db/db_impl/db_impl_compaction_flush.cc`：`MaybeScheduleFlushOrCompaction()`、`BackgroundCallCompaction()`、`BackgroundCompaction()`。
2. 同一文件：`CompactRange()`、`CompactRangeInternal()`、`RunManualCompaction()`、`CompactFiles()`、`CompactFilesImpl()`。
3. `db/db_impl/db_impl.cc`：`TriggerPeriodicCompaction()`、`CancelAllBackgroundWork()`。
4. `db/column_family.cc`：`ColumnFamilyData::NeedsCompaction()`、`ColumnFamilyData::PickCompaction()`，只作为调度后的选择边界。
5. `examples/compact_files_example.cc`、`db/compact_files_test.cc`、`db/db_test.cc`、`db/db_compaction_test.cc`：验证 API 入口、暂停、取消和并发时序。

本章不把 picker 算法、key reduction、输出安装或通用 write flow 复制为 canonical 内容。后续章节应链接到各自 owner。
