//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "file/compaction_io_experiment.h"

#include <algorithm>
#include <cassert>
#include <cinttypes>
#include <cstdio>

namespace ROCKSDB_NAMESPACE {

std::atomic<CompactionIOExperiment*> CompactionIOExperiment::active_{nullptr};

CompactionIOExperiment::CompactionIOExperiment(size_t depth,
                                               SystemClock* clock)
    : depth_(depth), clock_(clock) {
  assert(IsSupportedDepth(depth));
  assert(clock_ != nullptr);
  CompactionIOExperiment* expected = nullptr;
  const bool became_active =
      active_.compare_exchange_strong(expected, this, std::memory_order_release,
                                      std::memory_order_relaxed);
  assert(became_active);
  (void)became_active;
}

CompactionIOExperiment::~CompactionIOExperiment() {
  CompactionIOExperiment* expected = this;
  const bool became_inactive =
      active_.compare_exchange_strong(expected, nullptr,
                                      std::memory_order_release,
                                      std::memory_order_relaxed);
  assert(became_inactive);
  (void)became_inactive;
}

bool CompactionIOExperiment::IsSupportedDepth(uint64_t depth) {
  return depth == 1 || depth == 2 || depth == 4 || depth == 8 || depth == 16 ||
         depth == 32 || depth == 64;
}

CompactionIOExperiment* CompactionIOExperiment::Active() {
  return active_.load(std::memory_order_acquire);
}

void CompactionIOExperiment::RecordSynchronousRead(uint64_t requested_bytes,
                                                   uint64_t ready_bytes,
                                                   uint64_t service_micros) {
  read_count_.fetch_add(1, std::memory_order_relaxed);
  read_bytes_.fetch_add(requested_bytes, std::memory_order_relaxed);
  prefetched_bytes_.fetch_add(ready_bytes, std::memory_order_relaxed);
  ready_bytes_.fetch_add(ready_bytes, std::memory_order_relaxed);
  sync_fallback_read_micros_.Add(service_micros);
}

void CompactionIOExperiment::RecordAsynchronousReadIssued(
    uint64_t requested_bytes) {
  read_count_.fetch_add(1, std::memory_order_relaxed);
  read_bytes_.fetch_add(requested_bytes, std::memory_order_relaxed);
  outstanding_async_reads_.fetch_add(1, std::memory_order_relaxed);
}

void CompactionIOExperiment::RecordAsynchronousReadRejected(
    uint64_t requested_bytes) {
  read_count_.fetch_sub(1, std::memory_order_relaxed);
  read_bytes_.fetch_sub(requested_bytes, std::memory_order_relaxed);
  outstanding_async_reads_.fetch_sub(1, std::memory_order_relaxed);
}

void CompactionIOExperiment::RecordAsynchronousReadCompleted(
    uint64_t ready_bytes, uint64_t service_micros) {
  prefetched_bytes_.fetch_add(ready_bytes, std::memory_order_relaxed);
  ready_bytes_.fetch_add(ready_bytes, std::memory_order_relaxed);
  outstanding_async_reads_.fetch_sub(1, std::memory_order_relaxed);
  io_service_micros_.Add(service_micros);
}

void CompactionIOExperiment::RecordAsynchronousReadAborted() {
  outstanding_async_reads_.fetch_sub(1, std::memory_order_relaxed);
}

void CompactionIOExperiment::RecordConsumerDemand(uint64_t requested_bytes) {
  demand_count_.fetch_add(1, std::memory_order_relaxed);
  demand_bytes_.fetch_add(requested_bytes, std::memory_order_relaxed);
}

void CompactionIOExperiment::RecordConsumerPoll(uint64_t issue_micros,
                                                uint64_t demand_micros,
                                                uint64_t completion_micros,
                                                uint64_t poll_end_micros,
                                                bool was_waiting) {
  const bool causal_timing = issue_micros <= demand_micros &&
                             issue_micros <= completion_micros &&
                             demand_micros <= poll_end_micros &&
                             completion_micros <= poll_end_micros;
  if (causal_timing) {
    const uint64_t overlap_end = std::min(demand_micros, completion_micros);
    if (overlap_end > issue_micros) {
      const uint64_t overlap_micros = overlap_end - issue_micros;
      overlap_micros_.Add(overlap_micros);
      overlap_total_micros_.fetch_add(overlap_micros,
                                      std::memory_order_relaxed);
    }
    if (was_waiting && poll_end_micros > demand_micros) {
      const uint64_t exposed_io_micros = poll_end_micros - demand_micros;
      exposed_io_micros_.Add(exposed_io_micros);
      exposed_io_total_micros_.fetch_add(exposed_io_micros,
                                         std::memory_order_relaxed);
      read_block_count_.fetch_add(1, std::memory_order_relaxed);
    }
  }
  if (was_waiting) {
    stall_count_.fetch_add(1, std::memory_order_relaxed);
  }
}

void CompactionIOExperiment::RecordConsumed(uint64_t consumed_bytes) {
  consumed_bytes_.fetch_add(consumed_bytes, std::memory_order_relaxed);
}

void CompactionIOExperiment::RecordCompactionOutputWrite(
    uint64_t blocked_micros) {
  write_blocked_micros_.fetch_add(blocked_micros, std::memory_order_relaxed);
  write_block_count_.fetch_add(1, std::memory_order_relaxed);
}

void CompactionIOExperiment::RecordOutputSync(uint64_t sync_micros) {
  output_sync_micros_.fetch_add(sync_micros, std::memory_order_relaxed);
}

void CompactionIOExperiment::BeginCompactionJob(uint64_t job_id,
                                                uint64_t start_timestamp,
                                                int input_level,
                                                int output_level) {
  assert(!active_job_);
  active_job_ = true;
  active_job_metadata_.job_id = job_id;
  active_job_metadata_.start_timestamp = start_timestamp;
  active_job_metadata_.input_level = input_level;
  active_job_metadata_.output_level = output_level;
  active_job_baseline_.read_count =
      read_count_.load(std::memory_order_relaxed);
  active_job_baseline_.read_blocked_micros =
      exposed_io_total_micros_.load(std::memory_order_relaxed);
  active_job_baseline_.read_block_count =
      read_block_count_.load(std::memory_order_relaxed);
  active_job_baseline_.write_blocked_micros =
      write_blocked_micros_.load(std::memory_order_relaxed);
  active_job_baseline_.write_block_count =
      write_block_count_.load(std::memory_order_relaxed);
  active_job_baseline_.output_sync_micros =
      output_sync_micros_.load(std::memory_order_relaxed);
}

void CompactionIOExperiment::CompleteCompactionJob(
    uint64_t end_timestamp, uint64_t compaction_wall_us,
    uint64_t compaction_cpu_us, uint64_t logical_input_bytes,
    uint64_t logical_output_bytes) {
  if (!active_job_) {
    return;
  }
  JobRecord record = active_job_metadata_;
  record.end_timestamp = end_timestamp;
  record.compaction_wall_us = compaction_wall_us;
  record.compaction_cpu_us = compaction_cpu_us;
  record.logical_input_bytes = logical_input_bytes;
  record.logical_output_bytes = logical_output_bytes;
  record.read_count =
      read_count_.load(std::memory_order_relaxed) - active_job_baseline_.read_count;
  record.read_blocked_us = exposed_io_total_micros_.load(
                               std::memory_order_relaxed) -
                           active_job_baseline_.read_blocked_micros;
  record.read_block_count =
      read_block_count_.load(std::memory_order_relaxed) -
      active_job_baseline_.read_block_count;
  record.write_blocked_us =
      write_blocked_micros_.load(std::memory_order_relaxed) -
      active_job_baseline_.write_blocked_micros;
  record.write_block_count =
      write_block_count_.load(std::memory_order_relaxed) -
      active_job_baseline_.write_block_count;
  record.output_sync_us =
      output_sync_micros_.load(std::memory_order_relaxed) -
      active_job_baseline_.output_sync_micros;
  {
    std::lock_guard<std::mutex> lock(job_records_mutex_);
    job_records_.emplace_back(std::move(record));
  }
  active_job_ = false;
}

std::string CompactionIOExperiment::JobRecordsToJsonLines() const {
  std::lock_guard<std::mutex> lock(job_records_mutex_);
  std::string result;
  for (const auto& record : job_records_) {
    result.append(record.ToJson());
    result.push_back('\n');
  }
  return result;
}

void CompactionIOExperiment::RecordCompactionInterval(
    uint64_t compaction_time_micros, uint64_t cpu_time_micros) {
  compaction_time_micros_.fetch_add(compaction_time_micros,
                                    std::memory_order_relaxed);
  cpu_time_micros_.fetch_add(cpu_time_micros, std::memory_order_relaxed);
}

namespace {
std::string HistogramToJson(const HistogramImpl& histogram) {
  HistogramData data;
  histogram.Data(&data);
  char buffer[512];
  snprintf(buffer, sizeof(buffer),
           "{\"count\":%" PRIu64 ",\"avg\":%.6f,\"stddev\":%.6f,"
           "\"p50\":%.6f,\"p95\":%.6f,\"p99\":%.6f,\"max\":%.6f}",
           data.count, data.average, data.standard_deviation, data.median,
           data.percentile95, data.percentile99, data.max);
  return buffer;
}
}  // namespace

std::string CompactionIOExperiment::ToJson() const {
  char buffer[2000];
  size_t completed_job_count;
  {
    std::lock_guard<std::mutex> lock(job_records_mutex_);
    completed_job_count = job_records_.size();
  }
  snprintf(buffer, sizeof(buffer),
           "{\"compaction_io_experiment\":{\"depth\":%zu,"
           "\"request_span_bytes\":%zu,\"t_compaction_us\":%" PRIu64 ","
           "\"cpu_time_us\":%" PRIu64 ",\"read_count\":%" PRIu64 ","
           "\"read_bytes\":%" PRIu64 ",\"prefetched_bytes\":%" PRIu64
           ",\"ready_bytes\":%" PRIu64 ",\"consumed_bytes\":%" PRIu64
           ",\"demand_count\":%" PRIu64 ",\"demand_bytes\":%" PRIu64
           ",\"stall_count\":%" PRIu64 ",\"outstanding_async_reads\":%" PRIu64
            ",\"overlap_total_us\":%" PRIu64
            ",\"exposed_io_total_us\":%" PRIu64
            ",\"read_block_count\":%" PRIu64
            ",\"write_blocked_us\":%" PRIu64
            ",\"write_block_count\":%" PRIu64
            ",\"output_sync_us\":%" PRIu64
            ",\"completed_compaction_jobs\":%zu,"
            "\"io_service_us\":%s,\"sync_fallback_read_us\":%s,"
            "\"overlap_us\":%s,"
            "\"exposed_io_us\":%s}}",
           depth_, kRequestSpan,
           compaction_time_micros_.load(std::memory_order_relaxed),
           cpu_time_micros_.load(std::memory_order_relaxed),
           read_count_.load(std::memory_order_relaxed),
           read_bytes_.load(std::memory_order_relaxed),
           prefetched_bytes_.load(std::memory_order_relaxed),
           ready_bytes_.load(std::memory_order_relaxed),
           consumed_bytes_.load(std::memory_order_relaxed),
           demand_count_.load(std::memory_order_relaxed),
           demand_bytes_.load(std::memory_order_relaxed),
           stall_count_.load(std::memory_order_relaxed),
           outstanding_async_reads_.load(std::memory_order_relaxed),
           overlap_total_micros_.load(std::memory_order_relaxed),
            exposed_io_total_micros_.load(std::memory_order_relaxed),
            read_block_count_.load(std::memory_order_relaxed),
            write_blocked_micros_.load(std::memory_order_relaxed),
            write_block_count_.load(std::memory_order_relaxed),
            output_sync_micros_.load(std::memory_order_relaxed),
            completed_job_count,
           HistogramToJson(io_service_micros_).c_str(),
            HistogramToJson(sync_fallback_read_micros_).c_str(),
            HistogramToJson(overlap_micros_).c_str(),
           HistogramToJson(exposed_io_micros_).c_str());
  return buffer;
}

std::string CompactionIOExperiment::JobRecord::ToJson() const {
  char buffer[800];
  snprintf(buffer, sizeof(buffer),
           "{\"job_id\":%" PRIu64 ",\"start_timestamp\":%" PRIu64
           ",\"end_timestamp\":%" PRIu64 ",\"input_level\":%d,"
           "\"output_level\":%d,\"compaction_wall_us\":%" PRIu64
           ",\"compaction_cpu_us\":%" PRIu64
           ",\"logical_input_bytes\":%" PRIu64
           ",\"logical_output_bytes\":%" PRIu64
           ",\"read_blocked_us\":%" PRIu64
           ",\"read_block_count\":%" PRIu64
           ",\"read_count\":%" PRIu64
           ",\"write_blocked_us\":%" PRIu64
           ",\"write_block_count\":%" PRIu64
           ",\"output_sync_us\":%" PRIu64 "}",
           job_id, start_timestamp, end_timestamp, input_level, output_level,
           compaction_wall_us, compaction_cpu_us, logical_input_bytes,
           logical_output_bytes, read_blocked_us, read_block_count, read_count,
           write_blocked_us, write_block_count, output_sync_us);
  return buffer;
}

}  // namespace ROCKSDB_NAMESPACE
