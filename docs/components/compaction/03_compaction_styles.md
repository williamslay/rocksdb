# 03 Compaction Style 算法

本章只解释 picker 如何把候选状态变成 `Compaction`。共享的 score、overlap、clean cut、冲突和安装边界见 `02_scores_and_input_picking.md`；本章不把这些公共机制误写成某一种 style 的专属规则。

## 1. dispatch 总览

`ColumnFamilyData::PickCompaction` (`db/column_family.cc`) 把请求交给 `compaction_picker_`。具体实现由 `ColumnFamilyData` 创建时的 `compaction_style` 选择：`LevelCompactionPicker`、`UniversalCompactionPicker`、`FIFOCompactionPicker` 或 `NullCompactionPicker`。每个 picker 返回已经登记输入范围、输出 level 和 `CompactionReason` 的对象；返回 `nullptr` 表示本轮没有可执行候选。

| style | trigger / order | input model | output | blocking | trivial move | test anchor |
| --- | --- | --- | --- | --- | --- | --- |
| leveled | score >= 1 后按 score 降序；再 marked、bottommost、TTL、periodic、forced blob GC、read-triggered | 一个起始 level 文件，按 L0 overlap、output-level overlap 和 clean cut 扩展 | L0 -> dynamic `base_level`；其他 level 通常 -> next level；可 same-level | `being_compacted`、L0 in-progress、clean cut 失败、output range 冲突 | `TryPickL0TrivialMove` / `TryExtendNonL0TrivialMove` 先资格判断；`Compaction::IsTrivialMove` 再做硬门槛 | `db/compaction/compaction_picker_test.cc` 的 `LevelTrigger*`、`CompactionPriMinOverlapping*` |
| universal | periodic -> size amplification -> size-ratio -> sorted-run count -> delete-triggered -> read-triggered | 按时间顺序组织的 `SortedRun`；L0 是单文件，非零 level 聚合为一个 run | 选择连续 run，目标由后继 run level 或 `MaxOutputLevel` 决定 | run 被 compacting、standalone range tombstone 标记、output conflict、output-level requirement 不满足 | `allow_trivial_move` 且非 periodic 时检查 `IsInputFilesNonOverlapping` | `UniversalSizeAmp*`、`UniversalSizeRatio*`、`UniversalPeriodicCompaction*`、`UniversalReadTriggered*` |
| FIFO | TTL -> max-size drop -> intra-L0 -> temperature change | 以最老文件为删除候选；普通 FIFO 主要是 L0，迁移期扫描最底非空 level | 删除 compaction 的 output level 为 0；温度迁移也是一次一个文件 | 同时已有 L0 compaction、TTL 时间读取失败、容量条件不满足、文件正在 compacting | FIFO 删除路径不是 trivial move；它以 deletion compaction reason 交给执行层 | `db/compaction/compaction_picker_test.cc` 的 `NeedsCompactionFIFO`、`FIFOToCold*`；FIFO picker 测试 |
| null | 无 automatic trigger；`NeedsCompaction` 总是 false | 不创建输入 | 不创建 output | 无候选，两个 pick API 都直接返回 `nullptr` | 不适用 | `db/compaction/compaction_picker.h` 的实现是直接证据；没有把它误当 FIFO 的 picker 测试 |

## 2. Leveled

### 2.1 score、目标与顺序

`LevelCompactionBuilder::SetupInitialFiles` (`db/compaction/compaction_picker_level.cc`) 遍历已按 score 降序排列的 level。遇到 `start_level_score_ < 1` 就停止。分数触发时，L0 的 `output_level_` 是 `vstorage_->base_level()`，不是固定 L1；L1+ 通常是 `start_level_ + 1`。成功选择后 reason 是 L0 的 `kLevelL0FilesNum` 或其他 level 的 `kLevelMaxLevelSize`。

没有 score 候选时，顺序仍是算法的一部分：

1. `PickFilesMarkedForCompaction`。
2. `BottommostFilesMarkedForCompaction`。
3. TTL expired files。
4. periodic files。
5. forced blob GC files。
6. read-triggered files。

因此“TTL 总是先于 score”不成立。TTL 只在 score、marked 和 bottommost 路径没有返回可执行 compaction 后进入；`kRoundRobin` 另有一次以 expired list 首文件 level 为起点的尝试。`VersionStorageInfo::ComputeCompactionScore` (`db/version_set.cc`) 还会把 TTL boost 纳入 level picker 的文件排序，但这是本节仅适用于 leveled 的 `kMinOverlappingRatio` 选择路径细节，不是 universal 或 FIFO 的通用 boost 规则。

### 2.2 输入、扩展与阻塞

`PickFileToCompact` 先检查文件是否正在 compacting、是否跳过最后 level，再把单文件输入扩展成 clean cut。L0 -> 非 L0 时，`SetupOtherL0FilesIfNeeded` 调用 `CompactionPicker::GetOverlappingL0Files`，把重叠 L0 文件加入输入。随后 `SetupOtherInputsIfNeeded` 查询 output-level overlap；若 output range 与运行中 compaction 冲突，返回失败。非 trivial-move 路径还收集 grandparents，用于后续 output cutting，而 grandparents 不是额外 input。

`ExpandInputsToCleanCut` (`db/compaction/compaction_picker.cc`) 对非 L0 输入反复扩大范围，直到 user-key 边界完整；若扩大后发现 `AreFilesInCompaction`，本次候选被拒绝。L0 的 overlap 由 `GetOverlappingInputs` 直接处理。故 input 不是“score 对应的一个文件”这么简单，而是经过范围和并发约束后的集合。

### 2.3 intra-L0、round-robin 与 trivial move

当 L0 -> `base_level` 被阻塞，`PickIntraL0Compaction` 可选择从最新 L0 开始的连续 span，在 L0 内合并，以降低 file count 和 write stall 风险。`PickSizeBasedIntraL0Compaction` 在 L0 总量相对 base level 较小时使用 size 条件。它们 output level 都是 0，且不需要等待 L0 -> base 的 compaction。

`SetupOtherFilesWithRoundRobinExpansion` 只在 round-robin、非 L0 起始路径扩展连续文件。它不能跨过 compacting 文件或环绕到首文件；总 compaction bytes 受 `max_compaction_bytes`，扩展目标还参考 `MaxBytesForLevel`。已有 clean cut 和 output conflict 会停止扩展。`VersionStorageInfo::compact_cursor_` 负责持久 round-robin 游标；它不是候选数组索引。

trivial move 有两层含义。picker 侧的 `TryPickL0TrivialMove` 和 `TryExtendNonL0TrivialMove` 只生成可能直接搬移的候选；`Compaction::IsTrivialMove` (`db/compaction/compaction.cc`) 仍检查 level、输入数量、path、compression、grandparent overlap、SST partitioner 和 per-key placement。启用 per-key placement 时，普通 trivial move 会被拒绝。

### 2.4 leveled 证据表

| 维度 | 当前实现证据 |
| --- | --- |
| trigger | `LevelCompactionBuilder::SetupInitialFiles` 读取 `CompactionScore`，阈值是 `>= 1`；`LevelCompactionPicker::NeedsCompaction` 还报告 TTL、periodic、marked 等候选 |
| order | `SetupInitialFiles` 的 score loop，然后 marked -> bottommost -> TTL -> periodic -> forced blob GC -> read-triggered |
| input | `PickFileToCompact`、`GetOverlappingL0Files`、`SetupOtherInputsIfNeeded`、`ExpandInputsToCleanCut` |
| output | `base_level()` 处理 L0 动态目标；`start_level_ + 1` 处理一般 down-level；intra-L0 为 0 |
| blocking | `being_compacted`、`level0_compactions_in_progress`、clean-cut 失败、`FilesRangeOverlapWithCompaction` |
| trivial move | `TryPickL0TrivialMove`、`TryExtendNonL0TrivialMove`、`Compaction::IsTrivialMove` |
| test | `db/compaction/compaction_picker_test.cc`: `LevelTriggerDynamic4`, `CompactionPriMinOverlapping1`-`4`, `LevelCompactionPrioritizeFilesMarkedForCompaction1`-`2` |

## 3. Universal

### 3.1 sorted runs 与优先级

`UniversalCompactionBuilder::CalculateSortedRuns` (`db/compaction/compaction_picker_universal.cc`) 将 L0 每个文件表示为一个 `SortedRun`，将每个非零 level 的文件聚合为一个 run。`UniversalCompactionBuilder::PickCompaction` 计算 run、检查 trigger，然后按固定链式顺序调用：`MaybePickPeriodicCompaction`、`MaybePickSizeAmpCompaction`、`MaybePickCompactionToReduceSortedRunsBasedFileRatio`、`MaybePickCompactionToReduceSortedRuns`、`MaybePickDeleteTriggeredCompaction`、`MaybePickReadTriggeredCompaction`。前一个返回非空时，后续候选保持前一个结果，不会覆盖它。

### 3.2 size amplification、size ratio 与 stop style

`PickCompactionToReduceSizeAmp` 从最老 base run 向前找最长、未被阻塞的连续 span。它比较 newer runs 总大小 `candidate_size` 与 oldest/base run 的 `base_sr_size`：`candidate_size * 100 >= max_size_amplification_percent * base_sr_size` 才触发。该路径优先于 size-ratio 和 run-count，并可排除最新 L0 文件以降低 write-stop 风险。被 compacting 的 run、standalone range tombstone 标记或 output conflict 会截断/拒绝候选。

size-ratio 路径由 `PickCompactionToReduceSortedRuns` 实现。它从时间相邻的候选 run 开始，至少达到 `min_merge_width`，最多不超过 `max_merge_width` 和调用者限制；候选文件大小必须满足 ratio 条件。`size_ratio` 不是一个固定的“总和比较”概念：

| `stop_style` | `candidate_size` 如何推进 | 停止比较的含义 | auto `max_read_amp=0` |
| --- | --- | --- | --- |
| `kCompactionStopStyleTotalSize` | 累加已选 run 的 `compensated_file_size` | `(累计大小 * (100 + ratio) / 100) < 下一个 run size` 时停止 | 根据 `max_run_size_`、write buffer 和 ratio 估算 run 数 |
| `kCompactionStopStyleSimilarSize` | 每次改为最后一个候选 run 的 `compensated_file_size` | 除累计比较外，再检查当前候选不能比下一个 run 大超过 ratio；不相似则留待后续 outer loop | TODO 分支回退到 `file_num_compaction_trigger`，不使用 total-size auto tune |

这两个 stop style 使用不同 operand；不能用一条公式概括 universal 的 size ratio，也不能从源码推出普适 tuning value。run-count 路径由 `GetMaxNumFilesToCompactBasedOnMaxReadAmp` 根据 `max_read_amp` 决定；`max_read_amp < 0` 回退到 `level0_file_num_compaction_trigger`，`> 0` 是 run 数限制。

### 3.3 其他输入、输出与 trivial move

delete-triggered、periodic、read-triggered 候选通过各自 `Pick*Compaction` 形成 input。普通 run compaction 选择连续时间 span；非零 level run 会把该 level 的文件整体放入对应 `CompactionInputFiles`。incremental size-amp 路径可给 `PickIncrementalForReduceSizeAmp` 一个目标 fanout；否则回退到 full span。候选还必须通过 `MeetsOutputLevelRequirements` 和 `FilesRangeOverlapWithCompaction`。

`allow_trivial_move` 开启且 reason 不是 `kPeriodicCompaction` 时，`IsInputFilesNonOverlapping` 为真才会把 `is_trivial_move_` 写入 `Compaction`。这是 universal 自己的非重叠优化分支，不等于 leveled 的 `TryPickL0TrivialMove`，也不适用于 FIFO 的删除 compaction。

### 3.4 universal 证据表

| 维度 | 当前实现证据 |
| --- | --- |
| trigger | `UniversalCompactionBuilder::PickCompaction` 检查 run 数、periodic/marked/read-triggered 状态和 `level0_file_num_compaction_trigger` |
| order | `MaybePickPeriodicCompaction` -> `MaybePickSizeAmpCompaction` -> file-ratio -> run-count -> delete -> read |
| input | `CalculateSortedRuns`、`PickCompactionToReduceSizeAmp`、`PickCompactionToReduceSortedRuns`，连续时间 run 和非零 level 聚合文件 |
| output | `PickCompactionToReduceSortedRuns` 依据下一个 run level 或 `MaxOutputLevel` 设置 output；`BuildCompactionToNextLevel` 处理相邻层构造 |
| blocking | `being_compacted`、standalone range tombstone 标记、`MeetsOutputLevelRequirements`、output range overlap |
| trivial move | `allow_trivial_move` + `IsInputFilesNonOverlapping`；periodic compaction 明确排除 |
| test | `db/compaction/compaction_picker_test.cc`: `UniversalSizeAmpTierCompactionLastLevel`, `UniversalSizeRatioTierCompactionLastLevel`, `UniversalPeriodicCompaction1`-`6`, `UniversalReadTriggeredCompaction` |

## 4. FIFO

### 4.1 选择顺序

`FIFOCompactionPicker::PickCompaction` (`db/compaction/compaction_picker_fifo.cc`) 顺序固定为：

1. `PickTTLCompaction`，当 `ttl > 0`。
2. `PickSizeCompaction`，按有效 SST/blob 容量删除最老文件。
3. `PickIntraL0Compaction`，合并小 L0 文件降低 file count。
4. `PickTemperatureChangeCompaction`，从最老文件开始逐个改变温度。

前一阶段返回非空时，后续阶段不运行。`NeedsCompaction` 只看 L0 score `>= 1`；具体 TTL、容量和温度候选由 `PickCompaction` 进一步处理。

### 4.2 TTL、max-size 与迁移期输入

`PickTTLCompaction` 反向扫描 L0，按 estimated newest key time 找到 TTL 已过期文件。它在已有 L0 compaction 时返回 `nullptr`；没有过期文件，或删除后剩余有效数据仍高于 `GetEffectiveMax`，也返回 `nullptr`，让 size 路径继续处理。TTL deletion 的 output level 为 0，reason 是 `kFIFOTtl`。

`PickSizeCompaction` 计算所有 level 的 SST 总量，并在 `max_data_files_size > 0` 时加上 blob 总量。纯 L0 时从右侧 oldest 文件开始删除；迁移期存在非 L0 时选择最后非空 level，并从该 level 左侧开始删除，因为文件 creation time 不能代表 entry 的插入年龄。容量满足后返回 `kFIFOMaxSize` deletion compaction。正在 compacting 的文件会被跳过或阻止本轮形成完整候选。

FIFO 的 intra-L0 路径在容量删除没有返回时才执行；`PickIntraL0Compaction` 根据 `use_kv_ratio_compaction` 在 ratio-based 和 cost-based 策略间选择。temperature 路径 `PickTemperatureChangeCompaction` 一次选择一个最老文件，输出仍是 FIFO 的 L0 语义。

FIFO 不消费 `full_history_ts_low` 来提前触发 TTL；源码注释明确记录这是未完成 TODO。不要把 leveled 的 TTL boost、universal 的 size amp 或 FIFO 的 TTL deletion 合并成一个通用 TTL 算法。

### 4.3 FIFO 证据表

| 维度 | 当前实现证据 |
| --- | --- |
| trigger | `FIFOCompactionPicker::NeedsCompaction` 看 L0 score；`PickCompaction` 额外按 ttl、容量、file count、temperature 尝试 |
| order | `PickTTLCompaction` -> `PickSizeCompaction` -> `PickIntraL0Compaction` -> `PickTemperatureChangeCompaction` |
| input | TTL 取过期 L0；size 取 oldest L0 或迁移期最后非空 level；intra-L0 取小文件 span；temperature 一次一个 oldest file |
| output | FIFO picker `MaxOutputLevel()` 恒为 0；删除 compaction output level 为 0 |
| blocking | L0 compaction in progress、clock 读取失败、有效剩余容量仍超限、候选文件 compacting |
| trivial move | 删除 compaction 不走 trivial move；它用 `kFIFOTtl` / `kFIFOMaxSize` reason 表示删除 |
| test | `db/compaction/compaction_picker_test.cc`: `NeedsCompactionFIFO`, `FIFOToCold1`, `FIFOToColdMaxCompactionSize`, `FIFOToColdWithExistingCold`, `FIFOToColdWithHotBetweenCold`, `FIFOToHotAndWarm` |

## 5. Null picker

`NullCompactionPicker` (`db/compaction/compaction_picker.h`) 是禁用 automatic compaction 的 dummy 实现。`PickCompaction` 和 `PickCompactionForCompactRange` 都直接返回 `nullptr`，`NeedsCompaction` 直接返回 `false`。它不是 FIFO：不扫描文件、不删除过期数据、不改变温度，也不构造 deletion compaction。手动路径若需要具体输入，不能从 null picker 的返回值推导出 compaction。

| 维度 | 当前实现证据 |
| --- | --- |
| trigger / order | 无；两个 pick API 均立即返回 |
| input / output | 无输入、无 output、无 `Compaction` 对象 |
| blocking | “无候选”是定义行为，不是文件冲突 |
| trivial move | 不适用 |
| test | `db/compaction/compaction_picker.h` 的 inline implementation 是结构证据；本章不虚构 null-specific runtime test |

## 6. Tiered / proximal boundary

Tiered 不是第五个 `CompactionStyle` picker。现有 picker 先按 level 或 universal/FIFO 规则选择 compaction；tiered 的额外语义是 `Compaction::SupportsPerKeyPlacement`、`EvaluateProximalLevel`、`PopulateProximalLevelOutputRange` 等 proximal/per-key placement 逻辑，决定每个 key 是否可以向 proximal level 移动，以及 last level 中哪些 seqno 必须保留。

`Compaction::PopulateProximalLevelOutputRange` (`db/compaction/compaction.cc`) 在 per-key placement 开启时建立 safe range；universal 在 proximal level 的全部输入都被纳入时可扩展为 full range，否则使用 non-last range。`Compaction::IsTrivialMove` 也会因 `SupportsPerKeyPlacement` 拒绝普通 trivial move。这个层次改变 output placement 和保留边界，不改变 picker 的四分法。

证据锚点是 `db/compaction/tiered_compaction_test.cc`：测试覆盖 `output_to_proximal_level`、proximal range、last-level 保留、range tombstone 和 per-key placement；它们证明已有 compaction path 上的 placement contract，不证明存在 `TieredCompactionPicker`。与 `Compaction::SupportsPerKeyPlacement` 的调用关系也出现在 `compaction_job.cc`、`compaction_picker.cc` 和 output/cleanup 路径。

## 7. 阅读与校验边界

本章的算法结论均以当前 checkout 的 source path 和 symbol 为锚：

| source path | 本章用途 |
| --- | --- |
| `db/compaction/compaction_picker_level.cc` | leveled score order、L0/base、扩展、round-robin、intra-L0、trivial move |
| `db/compaction/compaction_picker_universal.cc` | sorted runs、size amp、size ratio、stop style、run count、delete/read/periodic、universal trivial move |
| `db/compaction/compaction_picker_fifo.cc` | FIFO TTL、容量删除、intra-L0、temperature |
| `db/compaction/compaction_picker.h` | picker interface 与 `NullCompactionPicker` |
| `db/compaction/compaction_picker.cc` | clean cut、L0 overlap、marked-file order、运行中冲突 |
| `db/version_set.cc` | score、TTL expired files、leveled file priority 和 TTL boost |
| `db/compaction/compaction.cc` | `Compaction::IsTrivialMove`、proximal/per-key placement |
| `db/compaction/compaction_picker_test.cc` | picker-specific test anchors |
| `db/compaction/tiered_compaction_test.cc` | tiered/proximal placement QA anchors |

测试名称是 coverage anchor，不是运行结果；本 docs-only 任务不运行 build、test、benchmark、stress 或 Jekyll。行号会随源码变化，symbol 和 path 是长期导航锚点。
