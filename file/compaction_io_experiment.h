//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

#include "monitoring/histogram.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

class CompactionIOExperiment {
 public:
  static constexpr size_t kRequestSpan = 1 << 20;

  CompactionIOExperiment(size_t depth, SystemClock* clock);
  ~CompactionIOExperiment();

  CompactionIOExperiment(const CompactionIOExperiment&) = delete;
  CompactionIOExperiment& operator=(const CompactionIOExperiment&) = delete;

  static bool IsSupportedDepth(uint64_t depth);
  static CompactionIOExperiment* Active();

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
  void RecordCompactionInterval(uint64_t compaction_time_micros,
                                uint64_t cpu_time_micros);

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
  std::atomic<uint64_t> outstanding_async_reads_{0};
  std::atomic<uint64_t> overlap_total_micros_{0};
  std::atomic<uint64_t> exposed_io_total_micros_{0};
  std::atomic<uint64_t> compaction_time_micros_{0};
  std::atomic<uint64_t> cpu_time_micros_{0};
  HistogramImpl io_service_micros_;
  HistogramImpl sync_fallback_read_micros_;
  HistogramImpl overlap_micros_;
  HistogramImpl exposed_io_micros_;
};

}  // namespace ROCKSDB_NAMESPACE
