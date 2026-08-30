# Compaction 分数与共享输入选择

本章只负责“候选是什么、如何排序、如何形成共享输入”。Leveled、Universal、FIFO、Null picker 的完整算法循环由 `03_compaction_styles.md` 负责。本章描述各 picker 共用的边界契约，不把某个 style 的循环复制到这里。

## 1. `VersionStorageInfo` 中的状态

`VersionStorageInfo::ComputeCompactionScore` 在持有 DB mutex 时重建 compaction 状态。每个 level 产生 `compaction_score_` 与 `compaction_level_`，随后按分数降序排列，`CompactionScore(0)` 对应当前最高优先级。分数 1.0 仍是触发阈值，动态 level 模式会把流入当前 level 的字节计入分母；因此它是当前 Version 的计算结果，不是固定配置值。

`base_level_` 由 Version 状态决定。动态 level bytes 开启时，L0 的输出目标由 `VersionStorageInfo::base_level()` 提供，不能写成“L0 总是到 L1”。`lowest_unnecessary_level_` 用于排空不再需要的 level。`max_compactions` 属于 `DBImpl::BGJobLimits` 计算出的后台作业限制，不是 `VersionStorageInfo` 状态，也不是 public option。

同一轮计算还刷新以下候选集合：`files_marked_for_compaction_`、`bottommost_files_marked_for_compaction_`、`expired_ttl_files_`（通过 `ExpiredTtlFiles()` 暴露）、`files_marked_for_periodic_compaction_`、forced blob GC 集合和 read-triggered 集合。文件的 `being_compacted` 状态会影响可计分字节和后续选择，不能只看文件总数。

## 2. 候选优先级与 dispatch

`ColumnFamilyData::PickCompaction` 把请求交给对应 `CompactionPicker`。Level picker 的 `LevelCompactionBuilder::SetupInitialFiles` 先遍历降序分数，分数低于 1.0 后停止，再按 marked、bottommost、TTL、periodic、forced blob GC、read-triggered 顺序尝试。marked-file 选择的第一次尝试使用 pseudo-random 起点，候选若与正在运行的 compaction 冲突则拒绝，并继续寻找可行文件。

这只是入口顺序，不等于所有候选都能成功形成 compaction。`PickFileToCompact` 会跳过最后 level 的不允许目标、正在进行 L0 compaction 的情况，并调用 `CompactionPicker::ExpandInputsToCleanCut`。失败的 clean cut 或冲突会清空本次输入，避免把不完整范围交给执行层。

## 3. 候选矩阵

| trigger | candidate list/state | picker dispatch | input scope | output target | blocking condition | compaction reason | test anchor |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 分数达到 1.0 | `compaction_score_` / `compaction_level_` | `ColumnFamilyData::PickCompaction` -> `LevelCompactionBuilder::SetupInitialFiles` | 先选 score level 文件，再按 overlap 扩展 | L0 到动态 `base_level`；其他 level 通常到下一 level | `being_compacted`、L0 in-progress、无法 clean cut | `kLevelL0FilesNum` 或 `kLevelMaxLevelSize` | `db/compaction/compaction_picker_test.cc` |
| marked | `files_marked_for_compaction_` | `PickFilesMarkedForCompaction` | marked 文件及必要 overlap | 由 picker 依据起始 level 决定 | pseudo-random 起点尝试后，冲突文件拒绝 | `kFilesMarkedForCompaction` | `db/compaction/compaction_picker_test.cc` |
| bottommost | `bottommost_files_marked_for_compaction_` | `PickFileToCompact`，same-level | bottommost marked 文件 | 起始 level，不向下层搬移 | 文件已 compacting 或 clean cut 失败 | `kBottommostFiles` | `db/compaction/compaction_picker_test.cc` |
| TTL | `expired_ttl_files_` / `ExpiredTtlFiles()` | `PickFileToCompact` | expired 文件，必要时 overlap；round-robin TTL 用 expired list 选择起始 level，但通用 `PickFileToCompact()` 后续仍可能选择该 level 的非-expired 文件 | 非末 level 可到下一 level；受 priority 分支影响 | 文件已 compacting、末 level跳过规则 | `kTtl`，round-robin 为 `kRoundRobinTtl` | `db/compaction/compaction_picker_test.cc` |
| periodic | `files_marked_for_periodic_compaction_` | `PickFileToCompact` | periodic 文件 | dynamic level bytes 时可向下一 level，否则 same-level | compacting 或 clean cut 失败 | `kPeriodicCompaction` | `db/compaction/compaction_picker_test.cc` |
| forced blob GC | forced blob GC candidate state | `PickFileToCompact` | 标记文件 | same-level | compacting 或 clean cut 失败 | `kForcedBlobGC` | `db/compaction/compaction_picker_test.cc` |
| read-triggered | `read_triggered_compaction_files_` | `PickFileToCompact` | read-triggered 文件 | picker 选择的下一 level | compacting 或 clean cut 失败 | `kReadTriggered` | `db/compaction/compaction_picker_test.cc` |

表中“输出 target”是 dispatch 契约，不是 tuning 建议。Universal/FIFO 的 style-specific 选择仍由 `03_compaction_styles.md` 解释。

## 4. overlap、clean cut 与冲突

`VersionStorageInfo::GetOverlappingInputs` 按 user-key 范围查询 level 文件。L0 的文件可能互相重叠，`expand_range` 会把相互覆盖的文件一并纳入；非 L0 level 通常保持排序的不重叠文件。`GetCleanInputsWithinInterval` 和 `CompactionPicker::ExpandInputsToCleanCut` 用于确保选择不会把同一 user key 的范围切在输入集合之外。

clean-cut 的完整 user-key invariant 是：输入范围边界不能把一个 user key 拆开，边界扩展必须覆盖该 key 的全部相关文件。它不是简单的 internal key 字符串截断。`LevelCompactionBuilder::SetupOtherInputsIfNeeded` 负责整个 other-input 阶段：它调用 `CompactionPicker::SetupOtherInputs` 查询 output level overlap 并扩展 clean cut，然后由 builder 自身调用 `FilesRangeOverlapWithCompaction` 检查运行中 compaction；若输出范围可能冲突，直接拒绝。

L0 到非 L0 输出时，`GetOverlappingL0Files` 会加入重叠 L0 文件。任何输入文件的 `being_compacted` 都是硬阻塞条件。非 exclusive manual compaction 或 ingest 造成的同 output-level 范围冲突，也必须拒绝，不能靠随后安装阶段补救。

## 5. grandparents、cursor 与 trivial move

`LevelCompactionBuilder::SetupOtherInputsIfNeeded` 对 `output_level_ == 0` 的路径（包括 intra-L0）直接只登记 start-level inputs，因此跳过 `SetupOtherInputs` 和 `GetGrandparents`。当输出 level 非 0 时，只有 picker 已预资格且设置 `is_l0_trivial_move_` 的 L0 trivial move 才跳过这两个步骤；其他路径仍由 builder 完成 setup，并在需要时收集 grandparents。它们不是新的 compaction input，而是输出切分和 overlap 约束的参考范围，交给 `Compaction` / output 层使用。具体切分循环由输出章节负责。builder 的 `FilesRangeOverlapWithCompaction` 冲突检查不因该 L0 trivial-move 例外而取消。

round-robin priority 持久化 `VersionStorageInfo::compact_cursor_`。`GetNextCompactCursor` 按本次输入文件数计算下一游标，安装结果时通过 `VersionEdit::AddCompactCursor` 写入，随后随 Version 状态推进。`next_file_to_compact_by_size_` 是 picker 的候选索引，不是持久 round-robin 游标。round-robin 扩展还要求连续文件、不能跨过 compacting 文件，并受 `max_compaction_bytes` 等当前 option 约束。循环细节留给 style 章节。

`Compaction::IsTrivialMove` 是执行前的硬门槛，不是“有一个文件就必然 trivial move”。除 L0 overlap、手动 compaction filter（`immutable_options_.compaction_filter` 或 `compaction_filter_factory`）、same-level 和 change-temperature 这些直接拒绝条件外，普通路径还要求 start/output level 不同、只有一个 input level、输入 path 与 output path 相同，并通过 `InputCompressionMatchesOutput()`；每个文件与其 grandparent overlap 的文件大小之和不能超过 `max_compaction_bytes_`，若存在 `SstPartitioner` 则每个文件的 key range 必须通过 `CanDoTrivialMove`，并且启用 per-key placement 时一律拒绝。Universal 的允许 trivial move 是单独分支：要求 `allow_trivial_move`、非 0 output level 和 Universal style，再返回 picker 已计算的 `is_trivial_move_`。这些 gate 说明能否直接移动，不等同于 picker 的全部资格检查。

## 6. 读代码顺序

建议按 `VersionStorageInfo::ComputeCompactionScore`、`ColumnFamilyData::PickCompaction`、`LevelCompactionBuilder::SetupInitialFiles`、`VersionStorageInfo::GetOverlappingInputs`、`CompactionPicker::ExpandInputsToCleanCut`、`LevelCompactionBuilder::SetupOtherInputsIfNeeded`、`CompactionPicker::SetupOtherInputs` 阅读。动态 level 的非 L0 分数还要注意，incoming bytes 只进入 overloaded branch 的 `MaxBytesForLevel(level) + total_downcompact_bytes` 分母。执行、输出和 style-specific 算法分别见 `04_input_io_and_memory.md`、`06_subcompactions_and_outputs.md`、`03_compaction_styles.md` 的对应章节。
