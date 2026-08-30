# Compaction 输入、I/O 与内存

本章只说明 compaction 输入侧怎样取得 SST、怎样选择缓存和预取路径，以及这些路径的内存与统计边界。通用 SST 查找见 [SST File Lookup](../read_flow/05_sst_file_lookup.md)，block cache 见 [Block Cache Integration](../read_flow/06_block_cache.md)，迭代器层次见 [Iterator and Scan Path](../read_flow/07_iterator_scan.md)，通用预取与异步 I/O 见 [Prefetching and Async I/O](../read_flow/10_prefetching_and_async_io.md)。

## 输入迭代器调用链

`CompactionJob::CreateInputIterator` (`db/compaction/compaction_job.cc`) 先把 `ReadOptions::verify_checksums` 打开、把 `fill_cache` 设为 `false`，并标记 `io_activity = kCompaction`。若启用 `use_direct_io_for_compaction_reads` 且未启用 `use_direct_reads`，它选择 compaction 输入文件选项，并把 `open_ephemeral_table_reader=true` 传下去。

随后调用 `VersionSet::MakeInputIterator` (`db/version_set.cc:8079`)。L0 为每个候选文件建立 child iterator，L1 及以上建立 `LevelIterator`，每个 table-cache 请求都带 `TableReaderCaller::kCompaction`。最后这些 child iterator 进入 compaction merging iterator。

`TableCache::NewIterator` (`db/table_cache.cc:350`) 打开或取得 table reader，再调用 table reader 的 `NewIterator`。Block-based table 由 `BlockBasedTableIterator` 消费 index 和 data block。这个 caller 身份会进入 `BlockCacheLookupContext`，因此 `BlockPrefetcher` 能区分 compaction 与 user scan。

`CreateInputIterator` 返回前，还可能包上 `ClippingIterator`、`BlobCountingIterator` 和 `HistoryTrimmingIterator`。这些 wrapper 持有底层 iterator，故 block、prefetch buffer 和 ephemeral reader 的释放时间通常由整个输入 iterator 栈的生命周期决定，而不是某个单独 `Read` 调用。

## Block cache 语义

`fill_cache=false` 是 compaction 的默认输入策略：读取出的 block 不应因扫描而污染 block cache。它抑制 cache insertion，但不关闭已有 block-cache lookup。lookup 与 admission 是两条独立决策：已有 entry 仍可命中并复用，miss 后读取出的 block 不会作为正常 entry 插入。

对 miss 的非缓存 data block，`BlockBasedTable::NewDataBlockIterator` 在 `fill_cache=false` 且存在 block cache 时，用 `Block::ApproximateMemoryUsage()` 插入唯一 dummy placeholder。这个 entry 只追踪该 iterator 的 charge，不是可复用的 data-block cache entry，并由 iterator cleanup 释放。随后 block ownership 通过 `CachableEntry::TransferTo` 转给 iterator；iterator 前进、reset 或销毁而清理当前 block 时，holder 和 placeholder 才能释放。因此不能把 `fill_cache=false` 写成“没有 lookup”或“零内存”。

## 五种机制的边界

| 机制 | owner | allocation/location | trigger | cache interaction | lifetime/direct-I/O behavior | statistic/caveat |
| --- | --- | --- | --- | --- | --- | --- |
| block cache | block-based table reader/cache | shared cache 中的 compressed 或 uncompressed block；非缓存 iterator block 另有 dummy charge | 先 lookup；miss 后按 admission 决定正常 insertion，`fill_cache=false` 禁止该 insertion | lookup 可启用；`fill_cache=false` 禁止正常 admission。非缓存 block 可由 `NewDataBlockIterator` 按 `ApproximateMemoryUsage()` 建立 iterator 绑定的 dummy placeholder | cached handle 或 dummy charge 由 iterator cleanup 管理；当前 data block 可在 iterator advance/reset 时释放 | cache hit/miss、block read 与 cache 统计，不等于所有读字节 |
| filesystem/page-cache prefetch | `FileSystem`/`RandomAccessFile` 实现 | OS 或 filesystem page cache，平台相关 | 非 direct-I/O compaction 读取先尝试 `BlockPrefetcher::PrefetchIfNeeded` 的 `Prefetch` | 不创建 RocksDB block-cache entry | direct I/O 跳过 filesystem prefetch；随后仍可进入 internal `FilePrefetchBuffer`，而非使用 OS page cache | 不能假设所有平台支持，也不能把它称为 RocksDB-owned DRAM |
| internal readahead buffer | `BlockPrefetcher` 创建的 `FilePrefetchBuffer` | RocksDB 进程内 `BufferInfo`/aligned buffer | FS prefetch 不支持或显式内部 readahead | 是读缓冲，不是 block cache admission | `BlockPrefetcher`/table iterator 持有；子 table iterator 清理时释放，析构时取消 pending async I/O、释放 buffers，并记录 discarded bytes | `PREFETCH_HITS` 与 `PREFETCH_BYTES_USEFUL` 仅在 `kUserScanPrefetch` 下记录；`READAHEAD_TRIMMED` 是 trim 事件计数，每次长度改变记 1，不是 trimmed bytes |
| direct I/O | compaction file options、file reader、`BlockFetcher` | `BlockFetcher::ReadBlock` 中的 aligned direct-I/O buffer；某些非压缩读取可借用 scoped allocator | `use_direct_io_for_compaction_reads` 选择 ephemeral table reader；compaction prefetch 分支因 direct I/O 跳过 filesystem `Prefetch` 并创建 internal `FilePrefetchBuffer` | 不依赖 OS page cache；仍可有 block lookup/cache policy | direct-I/O allocation 由 fetcher/read result 生命周期释放；internal `FilePrefetchBuffer` 随 child/table iterator 清理 | `block_read_count`、`block_read_byte` 等覆盖实际 block read，不代表 cache 命中 |
| blob readahead | blob fetcher/prefetch buffer collection | blob 文件 reader 的独立 readahead buffer | compaction 输入引用 blob，blob fetch 需要顺序读时 | 不等同 SST block cache，也不纳入 SST 的 block-cache 语义 | 由 blob iterator/fetcher 和 collection 持有，按其 reader 生命周期释放 | blob 读取统计与 garbage meter 分开；不要把 blob bytes 归入 `COMPACTION_PREFETCH_BYTES` |

`DBOptions::compaction_readahead_size` 当前 source-backed default 是 `2 * 1024 * 1024`，声明在 `include/rocksdb/options.h:1276`。`BlockPrefetcher` 注释明确该值只在 `lookup_context_.caller = kCompaction` 时使用。它不是所有 read-ahead 的总预算。

`COMPACTION_PREFETCH_BYTES` 只用于 internal compaction prefetch 的字节统计，并通过 histogram 记录。filesystem `Prefetch` 可能把页带入 OS page cache，计数和实现依赖 filesystem；不能把这部分、block cache、blob readahead 或任意用户 scan buffer 都加进该 histogram。也不能声称所有 buffer 都是 RocksDB-owned DRAM，direct I/O 和 filesystem page cache 的所有权不同。

## 预取与 direct I/O 的选择

`BlockPrefetcher::PrefetchIfNeeded` 以 `TableReaderCaller::kCompaction` 选择 `compaction_readahead_size`。非 direct-I/O 时优先调用 filesystem 的 `Prefetch`；direct I/O 跳过该分支，直接使用内部 `FilePrefetchBuffer`。若平台或 filesystem 不支持，也使用内部 `FilePrefetchBuffer`。`FilePrefetchBuffer::Prefetch` 复用已读范围，按 alignment 调整读区间；析构函数处理未完成 async 请求并统计未消费字节。该 buffer 跟随使用它的 child/table iterator，不应独立于 child iterator 延长到整个 compaction job。

## 证据边界

迭代器 reset 的清理路径是 `ResetDataIter`：它在当前 block 属于真实 block 时向 pinned-iterator manager 委托 cleanup、调用 `block_iter_.Invalidate(Status::OK())`，再清除真实 block 标记。`ClearBlockHandles` 只清空 readahead tuning 的 block-handle 元数据，不能替代 data-iterator cleanup；需要释放当前 block、dummy charge 或相关 prefetch state 时，应走 `ResetDataIter` 或实际 iterator cleanup 路径。

direct I/O 路径由 `CompactionJob::CreateInputIterator` 的两个 DB option 条件选择。`BlockFetcher::ReadBlock` 对 direct I/O 使用 aligned buffer，非 direct 路径则可能使用 filesystem scratch 或 block 自有 buffer。它们是不同 allocation/location，不能用“预取”统称。

Blob 输入走 `BlobCountingIterator` 后的 blob fetch/prefetch surface。其 readahead buffer 与 SST 的 `FilePrefetchBuffer` 分开管理，blob garbage accounting 也由 compaction wrapper 单独处理。

## 统计阅读规则

沿 `BlockFetcher::ReadBlock` 可看到 `block_read_count`、`block_read_byte` 以及按 block type 的读取计数。`FilePrefetchBuffer` 还可记录 useful bytes、hits、trimmed 事件和 discarded bytes，但这些计数口径不同：`UpdateStats` 只在 `kUserScanPrefetch` 下记录 hit 与 useful bytes；`UpdateReadAheadTrimmedStat` 在初始和更新长度不同的每次事件上对 `READAHEAD_TRIMMED` 加 1，不记录裁剪字节数。`COMPACTION_PREFETCH_BYTES` 因而只能支持 internal compaction prefetch 的窄声明，不能作为“所有 compaction I/O”指标。

排查时先按 caller、`fill_cache`、direct-I/O option 和 buffer owner 分层，再对照 [05](../read_flow/05_sst_file_lookup.md)、[06](../read_flow/06_block_cache.md)、[07](../read_flow/07_iterator_scan.md)、[10](../read_flow/10_prefetching_and_async_io.md) 的共享机制，避免重复解释或合并统计口径。
