# Compaction Source Atlas

This is the canonical navigation map for compaction. It declares exactly 18
paths. Each row has one owning chapter, one existing source path, one
representative symbol, one option or interface boundary, and one test or tool
anchor. Paths and symbols are navigation anchors, not runtime results.

| ID | canonical chapter | source path | representative symbol | option/interface | test/tool anchor |
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

## Boundary-only source families

These surfaces are included for boundary coverage, not promoted to additional
atlas rows or canonical compaction execution paths:

- Generated C API: generator inputs are `tools/c_api_gen/spec.json` and
  `tools/c_api_gen/c_base.h`; generated output boundary is `include/rocksdb/c.h`.
- JNI: bridge boundary is `java/rocksjni/compaction_job_info.cc`; Java API
  boundary is `java/src/main/java/org/rocksdb/CompactionJobInfo.java`.
- Unsupported or absent paths: no `include/rocksdb/compaction_service.h`, no
  `TieredCompactionPicker`, and no null-picker runtime claim beyond its inline
  source implementation.

## Scope

This atlas records current checkout paths and representative anchors only. No
build, test, benchmark, stress, db_bench, db_stress, or Jekyll command was run;
receipts below are structural path/ID checks. Existing dirty worktree paths
are baseline and outside this task. No product source, generated output,
plan, or Boulder file is changed.
