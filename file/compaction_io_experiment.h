//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "monitoring/histogram.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

class CompactionIOExperiment {
 public:
  struct JobRecord {
    uint64_t job_id = 0;
    uint64_t start_timestamp = 0;
    uint64_t end_timestamp = 0;
    int input_level = 0;
    int output_level = 0;
    uint64_t compaction_wall_us = 0;
    uint64_t compaction_cpu_us = 0;
    uint64_t logical_input_bytes = 0;
    uint64_t logical_output_bytes = 0;
    uint64_t read_blocked_us = 0;
    uint64_t read_block_count = 0;
    uint64_t read_count = 0;
    uint64_t write_blocked_us = 0;
    uint64_t write_block_count = 0;
    uint64_t output_sync_us = 0;

    std::string ToJson() const;
  };

  static constexpr size_t kRequestSpan = 1 << 20;

  CompactionIOExperiment(size_t depth, SystemClock* clock);
  ~CompactionIOExperiment();

  CompactionIOExperiment(const CompactionIOExperiment&) = delete;
  CompactionIOExperiment& operator=(const CompactionIOExperiment&) = delete;

  static bool IsSupportedDepth(uint64_t depth);
  static CompactionIOExperiment* Active();
  bool IsCompactionJobActive() const { return active_job_; }

  size_t depth() const { return depth_; }
  uint64_t NowMicros() const { return clock_->NowMicros(); }

  void RecordSynchronousRead(uint64_t requested_bytes, uint64_t ready_bytes,
                             uint64_t service_micros);
  void RecordAsynchronousReadIssued(uint64_t requested_bytes);
  void RecordAsynchronousReadRejected(uint64_t requested_bytes);
  void RecordAsynchronousReadCompleted(uint64_t ready_bytes,
                                       uint64_t service_micros);
  void RecordAsynchronousReadAborted();
  void RecordConsumerDemand(uint64_t requested_bytes);
  void RecordConsumerPoll(uint64_t issue_micros, uint64_t demand_micros,
                          uint64_t completion_micros, uint64_t poll_end_micros,
                          bool was_waiting);
  void RecordConsumed(uint64_t consumed_bytes);
  void RecordCompactionOutputWrite(uint64_t blocked_micros);
  void RecordOutputSync(uint64_t sync_micros);
  void RecordCompactionInterval(uint64_t compaction_time_micros,
                                uint64_t cpu_time_micros);

  void BeginCompactionJob(uint64_t job_id, uint64_t start_timestamp,
                          int input_level, int output_level);
  void CompleteCompactionJob(uint64_t end_timestamp,
                             uint64_t compaction_wall_us,
                             uint64_t compaction_cpu_us,
                             uint64_t logical_input_bytes,
                             uint64_t logical_output_bytes);
  std::string JobRecordsToJsonLines() const;

  uint64_t outstanding_async_reads() const {
    return outstanding_async_reads_.load(std::memory_order_relaxed);
  }
  std::string ToJson() const;

 private:
  static std::atomic<CompactionIOExperiment*> active_;

  const size_t depth_;
  SystemClock* const clock_;
  std::atomic<uint64_t> read_count_{0};
  std::atomic<uint64_t> read_bytes_{0};
  std::atomic<uint64_t> prefetched_bytes_{0};
  std::atomic<uint64_t> ready_bytes_{0};
  std::atomic<uint64_t> consumed_bytes_{0};
  std::atomic<uint64_t> demand_count_{0};
  std::atomic<uint64_t> demand_bytes_{0};
  std::atomic<uint64_t> stall_count_{0};
  std::atomic<uint64_t> read_block_count_{0};
  std::atomic<uint64_t> write_blocked_micros_{0};
  std::atomic<uint64_t> write_block_count_{0};
  std::atomic<uint64_t> output_sync_micros_{0};
  std::atomic<uint64_t> outstanding_async_reads_{0};
  std::atomic<uint64_t> overlap_total_micros_{0};
  std::atomic<uint64_t> exposed_io_total_micros_{0};
  std::atomic<uint64_t> compaction_time_micros_{0};
  std::atomic<uint64_t> cpu_time_micros_{0};
  HistogramImpl io_service_micros_;
  HistogramImpl sync_fallback_read_micros_;
  HistogramImpl overlap_micros_;
  HistogramImpl exposed_io_micros_;

  struct JobCounterSnapshot {
    uint64_t read_count = 0;
    uint64_t read_blocked_micros = 0;
    uint64_t read_block_count = 0;
    uint64_t write_blocked_micros = 0;
    uint64_t write_block_count = 0;
    uint64_t output_sync_micros = 0;
  };

  bool active_job_ = false;
  JobRecord active_job_metadata_;
  JobCounterSnapshot active_job_baseline_;
  mutable std::mutex job_records_mutex_;
  std::vector<JobRecord> job_records_;
};

}  // namespace ROCKSDB_NAMESPACE
