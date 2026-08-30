# Compaction 键归约语义

本章是 compaction 在输出前决定“保留、改写或丢弃”内部记录的 canonical 说明。SST 查找与 snapshot 基础见 [SuperVersion 与 snapshots](../read_flow/03_superversion_and_snapshots.md)，range deletion 见 [Range deletions](../read_flow/08_range_deletions.md)，merge 解析见 [Merge resolution](../read_flow/09_merge_resolution.md)，tombstone 生命周期见 [Tombstone lifecycle](../write_flow/08_tombstone_lifecycle.md)。本章不重复这些通用机制。

## 顺序、分层与输出

输入按 `InternalKeyComparator` 排序：user key 相等时，sequence number 倒序，timestamp 属于 user key 的比较语义。`CompactionIterator` 以 user-key group 为单位推进，`findEarliestVisibleSnapshot` 把有序 snapshots 切成可见性区间。`snapshots_` 为空表示 tip 可见，非空则同一 user key 可能需要为多个 snapshot stripe 保留历史版本。

| 输入条件 | visibility rule | emitted result | bottommost/timestamp caveat | source/test anchor |
| --- | --- | --- | --- | --- |
| 新 user key group | 先按内部键顺序消费，再按 snapshot stripe 判断 | 每个仍需服务的版本进入 `PrepareOutput` | `visible_at_tip_` 只表示 snapshots 为空，不等于可任意丢历史 | `db/compaction/compaction_iterator.cc` `NextFromInput`, `findEarliestVisibleSnapshot`; `db/compaction/compaction_iterator_test.cc` |
| 版本 sequence 高于对应 stripe 可见点 | 版本仍被较新记录遮蔽 | 跳过或继续归约 | snapshot 存在时不能用 bottommost 作为无条件删除理由 | `NextFromInput`; `db/dbformat.h` `ParsedInternalKey` |
| 版本跨越 `preserve_seqno_after_` | 低于保留边界的序号可能被归零 | 保留 value/type，按契约把 sequence 置零 | 构造器将 `preserve_seqno_min` 或 earliest snapshot 设为边界 | `compaction_iterator.cc` constructor; `compaction_iterator.h` |

序号归零不是删除：它只在不再需要旧序号区分时压缩 key 表示。保留 sequence 的条件由 snapshot、write-conflict snapshot、`preserve_seqno_min` 与 compaction 代理状态共同决定。输出的 key 必须仍能表达 value type、timestamp 和必要的可见性边界。

## 记录状态决策

| 输入条件 | visibility rule | emitted result | bottommost/timestamp caveat | source/test anchor |
| --- | --- | --- | --- | --- |
| `kTypeValue` 或实体值可见 | 当前 stripe 仍需值 | 输出原值，或 filter 改写值 | blob value 先按 resolver 合同解析；timestamp 不可从 user key 中抹掉 | `CompactionIterator::NextFromInput`, `InvokeFilterIfNeeded`; `db/compaction/compaction_iterator_test.cc` |
| 普通 deletion 已遮蔽更老值 | 只在所有相关 snapshots 都安全时消除 | 删除记录可省略，或保留作为 tombstone | bottommost 不是充分条件；存在旧 snapshot 时必须保留语义 | `NextFromInput`; `docs/components/write_flow/08_tombstone_lifecycle.md` |
| `kTypeSingleDeletion` | 必须与恰好一个匹配 value 配对 | 满足 SingleDelete contract 时两者可一起消除 | `enforce_single_del_contracts_` 控制违规处理，不能把 SingleDelete 当普通 deletion | `NextFromInput`, `CompactionIterator` constructor; `db/compaction/compaction_iterator_test.cc` |
| merge operands 可归约 | 交给 `MergeHelper`，按 snapshot 和 operand 顺序组成结果 | full merge 输出 value，或保留未完成 merge chain | merge filter 与普通 value filter 时机不同；通用 merge 规则见 read-flow 09 | `MergeHelper::MergeUntil`, `CompactionIterator::NextFromInput`; `db/merge_helper_test.cc` |
| merge 失败、shutdown 或 iterator error | 不产生伪造 value | 返回 status，停止正常输出 | 错误统计和取消路径属于 compaction job，不得当作删除成功 | `MergeHelper::MergeUntil`, `CompactionIterator::NextFromInput` |

`CompactionFilter` 只在记录已进入可过滤状态后调用。支持的 compaction filter 运行在 tip 语义下，忽略 snapshots；因此 `IgnoreSnapshots=false` 配置会被拒绝。`InvokeFilterIfNeeded` 根据 filter decision、`SupportsFilterV4` 和当前 key 状态决定删除、改值或 skip-until。`kRemoveAndSkipUntil` 使用 total-order seek 跳过目标 user-key 之前的输入，因此它也可以移除当前 snapshot 可见值。filter 仍不能绕过 range tombstone 判定，也不能把 blob reference 当成已解析的普通字节而违反 resolver contract。具体 API 语义链接 read/write 文档，不在此展开。

## Range tombstone 与 timestamp

`CompactionJob::CreateInputIterator` 先创建 `CompactionRangeDelAggregator`，再把它交给 `VersionSet::MakeInputIterator`。`CompactionRangeDelAggregator::AddTombstones` 对输入 tombstone 做边界截断、按 snapshots 的 `SplitBySnapshot` 分 stripe，并把 iterator fragment 放入对应 `StripeRep`。因此，**range-tombstone 的 timestamp/snapshot 过滤先于 sequence comparison**。只有通过 timestamp 上界、覆盖范围、snapshot stripe 和 key 匹配，才比较 tombstone sequence 与 key sequence。`ForwardRangeDelIterator::ShouldDelete` 和 reverse 版本最终使用 `tombstone_seq > key_seq`。

| 输入条件 | visibility rule | emitted result | bottommost/timestamp caveat | source/test anchor |
| --- | --- | --- | --- | --- |
| tombstone 覆盖 key，且位于当前 snapshot stripe | 先通过 timestamp/snapshot eligibility，再比较 sequence | key 可被 range deletion 隐藏 | fragment 只代表局部区间；不能把 iterator bounds 当覆盖保证 | `db/range_del_aggregator.cc` `AddTombstones`, `ForwardRangeDelIterator::ShouldDelete`; `db/range_del_aggregator_test.cc` |
| tombstone 新于 `trim_ts` | 不参与 `ShouldDelete` | 不能提前删除被覆盖 key | `trim_ts` bounds the original parent iterator, so `NewIterator` persistence also excludes newer tombstones | `CompactionRangeDelAggregator::AddTombstones`; `db/compaction/compaction_job.cc` `CreateInputIterator` |
| tombstone 新于 `full_history_ts_low` | GC 判定不使用它隐藏旧 key | 仍可作为输出 range tombstone 持久化 | 用户可能读取两者之间 timestamp，故不能 premature drop | `range_del_aggregator.cc` `AddTombstones` |
| tombstone 与 key 有范围重叠但不在覆盖关系 | 不删除 key | fragment 继续进入输出决策或输出 tombstone | `NewIterator` 的 internal bounds 只是优化，调用方仍需 enforce | `range_del_aggregator.h` `CompactionRangeDelAggregator::NewIterator` 注释 |
| bottommost 且无仍需服务的 snapshots | 才可按 tombstone 生命周期消除安全历史 | 可省略 tombstone 或被其覆盖的旧 key | “bottommost 所有 tombstone 都丢弃”错误，timestamp 与 snapshots 仍先决 | `CompactionIterator` `bottommost_level_`; `CompactionRangeDelAggregator::ShouldDelete` |

`trim_ts` 与 `full_history_ts_low` 是两条不同路径。显式 `trim_ts` 在 `CreateInputIterator` 中包裹 `HistoryTrimmingIterator`，该 iterator 对每次 seek/next/prev 过滤 timestamp 大于 `trim_ts` 的内部键；同时它 bounds the original parent range-tombstone iterator, so `CompactionRangeDelAggregator::NewIterator` cannot persist newer tombstones. 另一方面，`full_history_ts_low_` 传入 `CompactionIterator` 与 range aggregator，主要参与 GC eligibility 和 tombstone `ShouldDelete` 上界，不负责裁剪 parent iterator，也不控制 range-tombstone persistence；这项 persistence-only distinction 保留给 `full_history_ts_low`。两者可同时存在，aggregator 取更严格的 timestamp 上界，但这不把 input wrapper 的 trim 行为改名为 full-history GC。

## Blob 与 placement handoff

若 compaction 输入引用 blob，`CreateInputIterator` 在 clipping 后包上 `BlobCountingIterator`，并建立 `BlobGarbageMeter`。`CompactionBlobResolver` 在需要实体值或 filter 需要列值时通过 `BlobFetcher` 解析引用。输入流记录 inflow，输出流记录 outflow；`BlobGarbageMeter::BlobInOutFlow` 以 per-blob-file count/bytes 的差额报告新增 garbage。输出侧 `CompactionOutputs::CreateBlobGarbageMeter` 只为非-proximal output 创建 meter，故不能把 proximal output 的 placement 当 blob garbage accounting 的同一路径。

`ProximalOutputRangeType` 描述 output 是否允许进入 proximal level，`kNonLastRange` 还受 non-last-level input 范围约束。key 归约完成后，placement 决策把输出交给 proximal/per-key placement 管线；这不是第五种 picker，也不改变前述 snapshot、timestamp、tombstone 或 blob reference 语义。blob output creation 见 [subcompactions and outputs](06_subcompactions_and_outputs.md#builder-details-and-file-metadata)；安装与 additions 由后续 installation layer 负责。通用 tombstone 生命周期见 [Tombstone lifecycle](../write_flow/08_tombstone_lifecycle.md)。

## 不变量与排错

1. 同一 user key 的内部顺序不可被普通字符串排序替代，sequence、value type、timestamp 必须共同保持可解释性。
2. snapshot stripe 未耗尽前，不得因 bottommost 直接删除版本或 tombstone。
3. range tombstone 的 timestamp/snapshot eligibility 必须先于 sequence comparison。
4. `trim_ts` 的 iterator 过滤与 `full_history_ts_low` 的 GC 上界不可互换或合并描述。
5. filter、merge、blob resolver 的 status 失败不得伪装成空输出；SingleDelete contract 违规不得静默降级为普通 deletion。
6. blob inflow/outflow 必须按 reference 解析并按文件统计，不能用 SST key 数量代替 blob garbage。

排查时从 `CreateInputIterator` 的 wrapper 顺序开始，再看 `NextFromInput` 的当前 user-key 状态、`CompactionRangeDelAggregator::ShouldDelete` 的 stripe、最后看 `InvokeFilterIfNeeded` 和 `MergeHelper` status。通用 merge、range-delete、tombstone 机制分别沿本章顶部链接回读。
