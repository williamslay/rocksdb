//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "monitoring/histogram.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

class CompactionIOExperiment {
  struct SharedState;

 public:
  class JobContext {
   public:
    uint64_t NowMicros() const;
    uint64_t NowNanos() const;
    size_t depth() const;

    void RecordSynchronousRead(uint64_t requested_bytes, uint64_t ready_bytes,
                               uint64_t service_micros);
    void RecordAsynchronousReadIssued(uint64_t requested_bytes);
    void RecordAsynchronousReadRejected(uint64_t requested_bytes);
    void RecordAsynchronousReadCompleted(uint64_t ready_bytes,
                                         uint64_t service_micros);
    void RecordAsynchronousReadAborted();
    void RecordInputAsyncRejected(uint64_t requested_bytes, bool not_supported);
    void RecordInputAsyncSubmitted(uint64_t requested_bytes);
    struct InputAsyncToken {
     private:
      friend class JobContext;
      enum class Submission : uint8_t { kPending, kAccepted, kRejected };
      enum class Terminal : uint8_t { kNone, kReady, kZero, kAborted, kError };

      uint64_t requested_bytes = 0;
      uint64_t ready_bytes = 0;
      Submission submission = Submission::kPending;
      Terminal terminal = Terminal::kNone;
      bool terminal_seen = false;
      bool outstanding = false;
    };
    using InputAsyncTokenPtr = std::shared_ptr<InputAsyncToken>;
    InputAsyncTokenPtr BeginInputAsyncRead(uint64_t requested_bytes);
    void ResolveInputAsyncSubmission(const InputAsyncTokenPtr& token,
                                     bool accepted, bool not_supported);
    void RecordInputAsyncTerminal(const InputAsyncTokenPtr& token,
                                  uint64_t ready_bytes, bool succeeded,
                                  bool aborted);
    void RecordEffectivePrefetchDepth(size_t depth);
    void RecordWorkerThreadId(uint64_t worker_thread_id);
    void RecordInputAsyncCompleted(uint64_t requested_bytes,
                                   uint64_t ready_bytes);
    void RecordInputAsyncAborted(uint64_t requested_bytes, bool aborted);
    void RecordInputSyncFallback(uint64_t requested_bytes);
    void RecordConsumerDemand(uint64_t requested_bytes);
    void RecordConsumerPoll(uint64_t issue_micros, uint64_t demand_micros,
                            uint64_t completion_micros,
                            uint64_t poll_end_micros, bool was_waiting);
    void RecordConsumed(uint64_t consumed_bytes);
    void RecordCompactionOutputWrite(uint64_t blocked_micros);
    void RecordOutputSync(uint64_t sync_micros);

    uint64_t read_blocked_us() const {
      return read_blocked_us_.load(std::memory_order_relaxed);
    }
    uint64_t read_block_count() const {
      return read_block_count_.load(std::memory_order_relaxed);
    }
    uint64_t write_blocked_us() const {
      return write_blocked_us_.load(std::memory_order_relaxed);
    }
    uint64_t write_block_count() const {
      return write_block_count_.load(std::memory_order_relaxed);
    }
    uint64_t output_sync_us() const {
      return output_sync_us_.load(std::memory_order_relaxed);
    }

   public:
    explicit JobContext(std::shared_ptr<SharedState> state,
                        uint64_t worker_thread_id = 0);

   private:
    friend class CompactionIOExperiment;

    void RecordInputAsyncTerminalLocked(InputAsyncToken* token);

    std::shared_ptr<SharedState> state_;
    uint64_t worker_thread_id_ = 0;
    std::atomic<uint64_t> read_blocked_us_{0};
    std::atomic<uint64_t> read_block_count_{0};
    std::atomic<uint64_t> write_blocked_us_{0};
    std::atomic<uint64_t> write_block_count_{0};
    std::atomic<uint64_t> output_sync_us_{0};
  };

  using JobContextPtr = std::shared_ptr<JobContext>;

  class JobContextScope {
   public:
    explicit JobContextScope(const JobContextPtr& context);
    ~JobContextScope();

    JobContextScope(const JobContextScope&) = delete;
    JobContextScope& operator=(const JobContextScope&) = delete;

   private:
    JobContextPtr previous_;
  };

  struct Snapshot {
    uint64_t read_blocked_us_total = 0;
    uint64_t read_block_count_total = 0;
    uint64_t write_blocked_us_total = 0;
    uint64_t write_block_count_total = 0;
    uint64_t output_sync_us_total = 0;
    uint64_t output_sync_count_total = 0;
    uint64_t compaction_worker_us_total = 0;
    uint64_t logical_input_bytes_total = 0;
    uint64_t logical_output_bytes_total = 0;
    uint64_t active_compactions = 0;
    uint64_t compaction_lifecycle_count = 0;
    uint64_t input_uring_submit_count_total = 0;
    uint64_t input_uring_submit_bytes_total = 0;
    uint64_t input_uring_complete_count_total = 0;
    uint64_t input_uring_complete_bytes_total = 0;
    uint64_t input_uring_complete_request_bytes_total = 0;
    uint64_t input_uring_zero_byte_completion_count_total = 0;
    uint64_t input_uring_zero_byte_request_bytes_total = 0;
    uint64_t input_uring_rejected_count_total = 0;
    uint64_t input_uring_rejected_bytes_total = 0;
    uint64_t input_uring_not_supported_count_total = 0;
    uint64_t input_sync_fallback_count_total = 0;
    uint64_t input_sync_fallback_bytes_total = 0;
    uint64_t input_uring_abort_count_total = 0;
    uint64_t input_uring_abort_bytes_total = 0;
    uint64_t input_uring_error_count_total = 0;
    uint64_t input_uring_error_bytes_total = 0;
    uint64_t input_uring_outstanding_count = 0;
    uint64_t input_uring_outstanding_bytes = 0;
    uint64_t worker_thread_id = 0;
    uint64_t effective_prefetch_depth = 0;
    bool worker_ring_ready = false;
    bool witness_collected = false;
  };

  struct JobRecord {
    uint64_t thread_id = 0;
    uint64_t job_id = 0;
    uint64_t start_monotonic_us = 0;
    uint64_t start_epoch_us = 0;
    uint64_t end_monotonic_us = 0;
    uint64_t end_epoch_us = 0;
    uint64_t compaction_wall_us = 0;
    uint64_t logical_input_bytes = 0;
    std::string compaction_reason = "Unknown";
    int input_level = 0;
    int output_level = 0;
    uint64_t compaction_cpu_us = 0;
    uint64_t logical_output_bytes = 0;
    uint64_t read_blocked_us = 0;
    uint64_t read_block_count = 0;
    uint64_t write_blocked_us = 0;
    uint64_t write_block_count = 0;
    uint64_t output_sync_us = 0;
    std::string status = "OK";
    JobContextPtr context;

    std::string ToJson() const;
  };

  static constexpr size_t kRequestSpan = 1 << 20;

  CompactionIOExperiment(size_t depth, SystemClock* clock,
                         bool telemetry_enabled = true,
                         std::string jsonl_path = {},
                         bool witness_enabled = false);
  ~CompactionIOExperiment();

  CompactionIOExperiment(const CompactionIOExperiment&) = delete;
  CompactionIOExperiment& operator=(const CompactionIOExperiment&) = delete;

  static bool IsSupportedDepth(uint64_t depth);
  static std::shared_ptr<CompactionIOExperiment> Active();
  static JobContextPtr CurrentJobContext();
  static JobContextPtr CurrentOrActiveJobContext();

  bool telemetry_enabled() const;
  bool witness_enabled() const;
  size_t depth() const;
  uint64_t NowMicros() const;
  uint64_t NowNanos() const;

  void RecordSynchronousRead(uint64_t requested_bytes, uint64_t ready_bytes,
                             uint64_t service_micros);
  void RecordAsynchronousReadIssued(uint64_t requested_bytes);
  void RecordAsynchronousReadRejected(uint64_t requested_bytes);
  void RecordAsynchronousReadCompleted(uint64_t ready_bytes,
                                       uint64_t service_micros);
  void RecordAsynchronousReadAborted();
  void RecordInputAsyncRejected(uint64_t requested_bytes, bool not_supported);
  void RecordConsumerDemand(uint64_t requested_bytes);
  void RecordConsumerPoll(uint64_t issue_micros, uint64_t demand_micros,
                          uint64_t completion_micros, uint64_t poll_end_micros,
                          bool was_waiting);
  void RecordConsumed(uint64_t consumed_bytes);
  void RecordCompactionOutputWrite(uint64_t blocked_micros);
  void RecordOutputSync(uint64_t sync_micros);
  void RecordCompactionInterval(uint64_t compaction_time_micros,
                                uint64_t cpu_time_micros);

  Snapshot GetSnapshot() const;
  Snapshot GetSnapshotAt(uint64_t monotonic_us) const;
  bool RecordRuntimeMaxSubcompactions(uint32_t max_subcompactions);

  JobContextPtr BeginCompactionJob(uint64_t job_id, uint64_t start_monotonic_us,
                                   uint64_t start_epoch_us, uint64_t thread_id,
                                   int input_level, int output_level,
                                   const std::string& compaction_reason);
  void CompleteCompactionJob(uint64_t job_id, uint64_t end_monotonic_us,
                             uint64_t end_epoch_us, uint64_t compaction_wall_us,
                             uint64_t compaction_cpu_us,
                             uint64_t logical_input_bytes,
                             uint64_t logical_output_bytes, bool succeeded);
  std::string JobRecordsToJsonLines() const;

  uint64_t outstanding_async_reads() const;
  std::string ToJson() const;

 private:
  friend Status ReleaseCompactionIOExperiment();

  Status WriteJobRecordsToJsonl() const;
  JobContextPtr ContextForRecord() const;

  static thread_local JobContextPtr current_job_context_;

  const size_t depth_;
  SystemClock* const clock_;
  const bool telemetry_enabled_;
  const bool witness_enabled_;
  const std::string jsonl_path_;
  const std::shared_ptr<SharedState> state_;
  const JobContextPtr legacy_context_;

  mutable std::mutex active_jobs_mutex_;
  struct ActiveJob {
    JobRecord metadata;
    JobContextPtr context;
  };
  std::unordered_map<uint64_t, ActiveJob> active_jobs_;
  uint64_t active_compactions_ = 0;
  uint64_t last_transition_monotonic_us_ = 0;
  uint64_t accumulated_compaction_worker_us_ = 0;
  uint64_t compaction_lifecycle_count_ = 0;
  uint32_t runtime_max_subcompactions_ = 1;
  bool max_subcompactions_valid_ = true;
  mutable std::mutex job_records_mutex_;
  std::vector<JobRecord> job_records_;

  void AdvanceWorkerTimeLocked(uint64_t monotonic_us);
  Snapshot GetSnapshotAtLocked(uint64_t monotonic_us) const;
};

Status AcquireCompactionIOExperiment(uint64_t depth, bool telemetry_enabled);
Status AcquireCompactionIOExperimentFromEnvironment();
Status ReleaseCompactionIOExperiment();

}  // namespace ROCKSDB_NAMESPACE
