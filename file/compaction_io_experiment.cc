//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "file/compaction_io_experiment.h"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cerrno>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <utility>

namespace ROCKSDB_NAMESPACE {

struct CompactionIOExperiment::SharedState {
  SharedState(size_t depth_arg, SystemClock* clock_arg,
              bool telemetry_enabled_arg, bool witness_enabled_arg)
      : depth(depth_arg),
        clock(clock_arg),
        telemetry_enabled(telemetry_enabled_arg),
        witness_enabled(witness_enabled_arg) {}

  const size_t depth;
  SystemClock* const clock;
  const bool telemetry_enabled;
  const bool witness_enabled;
  std::atomic<uint64_t> read_count{0};
  std::atomic<uint64_t> read_bytes{0};
  std::atomic<uint64_t> prefetched_bytes{0};
  std::atomic<uint64_t> ready_bytes{0};
  std::atomic<uint64_t> consumed_bytes{0};
  std::atomic<uint64_t> demand_count{0};
  std::atomic<uint64_t> demand_bytes{0};
  std::atomic<uint64_t> stall_count{0};
  std::atomic<uint64_t> read_block_count_total{0};
  std::atomic<uint64_t> read_blocked_us_total{0};
  std::atomic<uint64_t> write_blocked_us_total{0};
  std::atomic<uint64_t> write_block_count_total{0};
  std::atomic<uint64_t> output_sync_us_total{0};
  std::atomic<uint64_t> output_sync_count_total{0};
  std::atomic<uint64_t> outstanding_async_reads{0};
  std::atomic<uint64_t> input_uring_submit_count_total{0};
  std::atomic<uint64_t> input_uring_submit_bytes_total{0};
  std::atomic<uint64_t> input_uring_complete_count_total{0};
  std::atomic<uint64_t> input_uring_complete_bytes_total{0};
  std::atomic<uint64_t> input_uring_complete_request_bytes_total{0};
  std::atomic<uint64_t> input_uring_zero_byte_completion_count_total{0};
  std::atomic<uint64_t> input_uring_zero_byte_request_bytes_total{0};
  std::atomic<uint64_t> input_uring_rejected_count_total{0};
  std::atomic<uint64_t> input_uring_rejected_bytes_total{0};
  std::atomic<uint64_t> input_uring_not_supported_count_total{0};
  std::atomic<uint64_t> input_sync_fallback_count_total{0};
  std::atomic<uint64_t> input_sync_fallback_bytes_total{0};
  std::atomic<uint64_t> input_uring_abort_count_total{0};
  std::atomic<uint64_t> input_uring_abort_bytes_total{0};
  std::atomic<uint64_t> input_uring_error_count_total{0};
  std::atomic<uint64_t> input_uring_error_bytes_total{0};
  std::atomic<uint64_t> input_uring_outstanding_count{0};
  std::atomic<uint64_t> input_uring_outstanding_bytes{0};
  std::atomic<uint64_t> effective_prefetch_depth{0};
  std::mutex input_uring_witness_mutex;
  std::atomic<uint64_t> worker_thread_id{0};
  std::atomic<bool> worker_ring_ready{false};
  std::atomic<uint64_t> overlap_total_micros{0};
  std::atomic<uint64_t> exposed_io_total_micros{0};
  std::atomic<uint64_t> compaction_time_micros{0};
  std::atomic<uint64_t> cpu_time_micros{0};
  std::atomic<uint64_t> logical_input_bytes_total{0};
  std::atomic<uint64_t> logical_output_bytes_total{0};
  HistogramImpl io_service_micros;
  HistogramImpl sync_fallback_read_micros;
  HistogramImpl overlap_micros;
  HistogramImpl exposed_io_micros;
};

thread_local CompactionIOExperiment::JobContextPtr
    CompactionIOExperiment::current_job_context_ = nullptr;

namespace {

std::mutex& CompactionIOExperimentOwnerMutex() {
  static std::mutex mutex;
  return mutex;
}

std::shared_ptr<CompactionIOExperiment>& CompactionIOExperimentOwner() {
  static std::shared_ptr<CompactionIOExperiment> owner;
  return owner;
}

void RegisterCompactionIOExperimentAtExit() {
  static const bool registered = []() {
    std::atexit([] { (void)ReleaseCompactionIOExperiment(); });
    return true;
  }();
  (void)registered;
}

Status PublishCompactionIOExperiment(
    std::shared_ptr<CompactionIOExperiment> experiment) {
  std::lock_guard<std::mutex> lock(CompactionIOExperimentOwnerMutex());
  if (CompactionIOExperimentOwner() != nullptr) {
    return Status::Busy("compaction I/O experiment owner already active");
  }
  CompactionIOExperimentOwner() = std::move(experiment);
  RegisterCompactionIOExperimentAtExit();
  return Status::OK();
}

}  // namespace

CompactionIOExperiment::JobContext::JobContext(
    std::shared_ptr<SharedState> state, uint64_t worker_thread_id)
    : state_(std::move(state)) {
  if (state_->witness_enabled) {
    std::lock_guard<std::mutex> lock(state_->input_uring_witness_mutex);
    worker_thread_id_ = worker_thread_id;
    if (state_->worker_thread_id.load(std::memory_order_relaxed) !=
        worker_thread_id) {
      state_->worker_ring_ready.store(false, std::memory_order_relaxed);
    }
    state_->worker_thread_id.store(worker_thread_id, std::memory_order_relaxed);
  }
}

uint64_t CompactionIOExperiment::JobContext::NowMicros() const {
  return state_->telemetry_enabled ? state_->clock->NowMicros() : 0;
}

uint64_t CompactionIOExperiment::JobContext::NowNanos() const {
  return state_->telemetry_enabled ? state_->clock->NowNanos() : 0;
}

size_t CompactionIOExperiment::JobContext::depth() const {
  return state_->depth;
}

void CompactionIOExperiment::JobContext::RecordSynchronousRead(
    uint64_t requested_bytes, uint64_t ready_bytes, uint64_t service_micros) {
  if (!state_->telemetry_enabled) {
    return;
  }
  state_->read_count.fetch_add(1, std::memory_order_relaxed);
  state_->read_bytes.fetch_add(requested_bytes, std::memory_order_relaxed);
  state_->prefetched_bytes.fetch_add(ready_bytes, std::memory_order_relaxed);
  state_->ready_bytes.fetch_add(ready_bytes, std::memory_order_relaxed);
  state_->sync_fallback_read_micros.Add(service_micros);
}

void CompactionIOExperiment::JobContext::RecordAsynchronousReadIssued(
    uint64_t requested_bytes) {
  if (!state_->telemetry_enabled) {
    return;
  }
  state_->read_count.fetch_add(1, std::memory_order_relaxed);
  state_->read_bytes.fetch_add(requested_bytes, std::memory_order_relaxed);
  state_->outstanding_async_reads.fetch_add(1, std::memory_order_relaxed);
}

void CompactionIOExperiment::JobContext::RecordAsynchronousReadRejected(
    uint64_t requested_bytes) {
  if (!state_->telemetry_enabled) {
    return;
  }
  state_->read_count.fetch_sub(1, std::memory_order_relaxed);
  state_->read_bytes.fetch_sub(requested_bytes, std::memory_order_relaxed);
  state_->outstanding_async_reads.fetch_sub(1, std::memory_order_relaxed);
}

void CompactionIOExperiment::JobContext::RecordInputAsyncRejected(
    uint64_t requested_bytes, bool not_supported) {
  if (!state_->witness_enabled) {
    return;
  }
  std::lock_guard<std::mutex> lock(state_->input_uring_witness_mutex);
  state_->input_uring_rejected_count_total.fetch_add(1,
                                                     std::memory_order_relaxed);
  state_->input_uring_rejected_bytes_total.fetch_add(requested_bytes,
                                                     std::memory_order_relaxed);
  if (not_supported) {
    state_->input_uring_not_supported_count_total.fetch_add(
        1, std::memory_order_relaxed);
  }
}

void CompactionIOExperiment::JobContext::RecordAsynchronousReadCompleted(
    uint64_t ready_bytes, uint64_t service_micros) {
  if (!state_->telemetry_enabled) {
    return;
  }
  state_->prefetched_bytes.fetch_add(ready_bytes, std::memory_order_relaxed);
  state_->ready_bytes.fetch_add(ready_bytes, std::memory_order_relaxed);
  state_->outstanding_async_reads.fetch_sub(1, std::memory_order_relaxed);
  state_->io_service_micros.Add(service_micros);
}

void CompactionIOExperiment::JobContext::RecordAsynchronousReadAborted() {
  if (state_->telemetry_enabled) {
    state_->outstanding_async_reads.fetch_sub(1, std::memory_order_relaxed);
  }
}

void CompactionIOExperiment::JobContext::RecordInputAsyncSubmitted(
    uint64_t requested_bytes) {
  if (!state_->witness_enabled) {
    return;
  }
  std::lock_guard<std::mutex> lock(state_->input_uring_witness_mutex);
  state_->input_uring_submit_count_total.fetch_add(1,
                                                   std::memory_order_relaxed);
  state_->input_uring_submit_bytes_total.fetch_add(requested_bytes,
                                                   std::memory_order_relaxed);
  state_->input_uring_outstanding_count.fetch_add(1, std::memory_order_relaxed);
  state_->input_uring_outstanding_bytes.fetch_add(requested_bytes,
                                                  std::memory_order_relaxed);
  if (worker_thread_id_ != 0 &&
      worker_thread_id_ ==
          state_->worker_thread_id.load(std::memory_order_relaxed)) {
    state_->worker_ring_ready.store(true, std::memory_order_relaxed);
  }
}

CompactionIOExperiment::JobContext::InputAsyncTokenPtr
CompactionIOExperiment::JobContext::BeginInputAsyncRead(
    uint64_t requested_bytes) {
  if (!state_->witness_enabled) {
    return nullptr;
  }
  auto token = std::make_shared<InputAsyncToken>();
  token->requested_bytes = requested_bytes;
  return token;
}

void CompactionIOExperiment::JobContext::ResolveInputAsyncSubmission(
    const InputAsyncTokenPtr& token, bool accepted, bool not_supported) {
  if (token == nullptr || !state_->witness_enabled) {
    return;
  }
  std::lock_guard<std::mutex> lock(state_->input_uring_witness_mutex);
  if (token->submission != InputAsyncToken::Submission::kPending) {
    return;
  }
  if (!accepted) {
    token->submission = InputAsyncToken::Submission::kRejected;
    state_->input_uring_rejected_count_total.fetch_add(
        1, std::memory_order_relaxed);
    state_->input_uring_rejected_bytes_total.fetch_add(
        token->requested_bytes, std::memory_order_relaxed);
    if (not_supported) {
      state_->input_uring_not_supported_count_total.fetch_add(
          1, std::memory_order_relaxed);
      state_->input_sync_fallback_count_total.fetch_add(
          1, std::memory_order_relaxed);
      state_->input_sync_fallback_bytes_total.fetch_add(
          token->requested_bytes, std::memory_order_relaxed);
    }
    return;
  }

  token->submission = InputAsyncToken::Submission::kAccepted;
  state_->input_uring_submit_count_total.fetch_add(1,
                                                   std::memory_order_relaxed);
  state_->input_uring_submit_bytes_total.fetch_add(token->requested_bytes,
                                                   std::memory_order_relaxed);
  if (worker_thread_id_ != 0 &&
      worker_thread_id_ ==
          state_->worker_thread_id.load(std::memory_order_relaxed)) {
    state_->worker_ring_ready.store(true, std::memory_order_relaxed);
  }
  if (token->terminal_seen) {
    RecordInputAsyncTerminalLocked(token.get());
  } else {
    token->outstanding = true;
    state_->input_uring_outstanding_count.fetch_add(1,
                                                    std::memory_order_relaxed);
    state_->input_uring_outstanding_bytes.fetch_add(token->requested_bytes,
                                                    std::memory_order_relaxed);
  }
}

void CompactionIOExperiment::JobContext::RecordInputAsyncTerminal(
    const InputAsyncTokenPtr& token, uint64_t ready_bytes, bool succeeded,
    bool aborted) {
  if (token == nullptr || !state_->witness_enabled) {
    return;
  }
  std::lock_guard<std::mutex> lock(state_->input_uring_witness_mutex);
  if (token->terminal_seen ||
      token->submission == InputAsyncToken::Submission::kRejected) {
    return;
  }
  token->terminal_seen = true;
  token->ready_bytes = ready_bytes;
  token->terminal = succeeded
                        ? (ready_bytes > 0 ? InputAsyncToken::Terminal::kReady
                                           : InputAsyncToken::Terminal::kZero)
                        : (aborted ? InputAsyncToken::Terminal::kAborted
                                   : InputAsyncToken::Terminal::kError);
  if (token->submission == InputAsyncToken::Submission::kAccepted) {
    RecordInputAsyncTerminalLocked(token.get());
  }
}

void CompactionIOExperiment::JobContext::RecordInputAsyncTerminalLocked(
    InputAsyncToken* token) {
  if (token->submission != InputAsyncToken::Submission::kAccepted ||
      !token->terminal_seen) {
    return;
  }
  if (token->outstanding) {
    state_->input_uring_outstanding_count.fetch_sub(1,
                                                    std::memory_order_relaxed);
    state_->input_uring_outstanding_bytes.fetch_sub(token->requested_bytes,
                                                    std::memory_order_relaxed);
    token->outstanding = false;
  }
  switch (token->terminal) {
    case InputAsyncToken::Terminal::kReady:
      state_->input_uring_complete_count_total.fetch_add(
          1, std::memory_order_relaxed);
      state_->input_uring_complete_bytes_total.fetch_add(
          token->ready_bytes, std::memory_order_relaxed);
      state_->input_uring_complete_request_bytes_total.fetch_add(
          token->requested_bytes, std::memory_order_relaxed);
      break;
    case InputAsyncToken::Terminal::kZero:
      state_->input_uring_zero_byte_completion_count_total.fetch_add(
          1, std::memory_order_relaxed);
      state_->input_uring_zero_byte_request_bytes_total.fetch_add(
          token->requested_bytes, std::memory_order_relaxed);
      break;
    case InputAsyncToken::Terminal::kAborted:
      state_->input_uring_abort_count_total.fetch_add(
          1, std::memory_order_relaxed);
      state_->input_uring_abort_bytes_total.fetch_add(
          token->requested_bytes, std::memory_order_relaxed);
      break;
    case InputAsyncToken::Terminal::kError:
      state_->input_uring_error_count_total.fetch_add(
          1, std::memory_order_relaxed);
      state_->input_uring_error_bytes_total.fetch_add(
          token->requested_bytes, std::memory_order_relaxed);
      break;
    case InputAsyncToken::Terminal::kNone:
      assert(false);
      break;
  }
}

void CompactionIOExperiment::JobContext::RecordEffectivePrefetchDepth(
    size_t depth) {
  if (!state_->witness_enabled) {
    return;
  }
  std::lock_guard<std::mutex> lock(state_->input_uring_witness_mutex);
  state_->effective_prefetch_depth.store(depth, std::memory_order_relaxed);
}

void CompactionIOExperiment::JobContext::RecordWorkerThreadId(
    uint64_t worker_thread_id) {
  if (!state_->witness_enabled) {
    return;
  }
  std::lock_guard<std::mutex> lock(state_->input_uring_witness_mutex);
  const uint64_t previous =
      state_->worker_thread_id.load(std::memory_order_relaxed);
  if (previous != worker_thread_id) {
    state_->worker_ring_ready.store(false, std::memory_order_relaxed);
  }
  worker_thread_id_ = worker_thread_id;
  state_->worker_thread_id.store(worker_thread_id, std::memory_order_relaxed);
}

void CompactionIOExperiment::JobContext::RecordInputAsyncCompleted(
    uint64_t requested_bytes, uint64_t ready_bytes) {
  if (!state_->witness_enabled) {
    return;
  }
  std::lock_guard<std::mutex> lock(state_->input_uring_witness_mutex);
  state_->input_uring_outstanding_count.fetch_sub(1, std::memory_order_relaxed);
  state_->input_uring_outstanding_bytes.fetch_sub(requested_bytes,
                                                  std::memory_order_relaxed);
  if (ready_bytes > 0) {
    state_->input_uring_complete_count_total.fetch_add(
        1, std::memory_order_relaxed);
    state_->input_uring_complete_bytes_total.fetch_add(
        ready_bytes, std::memory_order_relaxed);
    state_->input_uring_complete_request_bytes_total.fetch_add(
        requested_bytes, std::memory_order_relaxed);
  } else {
    state_->input_uring_zero_byte_completion_count_total.fetch_add(
        1, std::memory_order_relaxed);
    state_->input_uring_zero_byte_request_bytes_total.fetch_add(
        requested_bytes, std::memory_order_relaxed);
  }
}

void CompactionIOExperiment::JobContext::RecordInputAsyncAborted(
    uint64_t requested_bytes, bool aborted) {
  if (!state_->witness_enabled) {
    return;
  }
  std::lock_guard<std::mutex> lock(state_->input_uring_witness_mutex);
  state_->input_uring_outstanding_count.fetch_sub(1, std::memory_order_relaxed);
  state_->input_uring_outstanding_bytes.fetch_sub(requested_bytes,
                                                  std::memory_order_relaxed);
  if (aborted) {
    state_->input_uring_abort_count_total.fetch_add(1,
                                                    std::memory_order_relaxed);
    state_->input_uring_abort_bytes_total.fetch_add(requested_bytes,
                                                    std::memory_order_relaxed);
  } else {
    state_->input_uring_error_count_total.fetch_add(1,
                                                    std::memory_order_relaxed);
    state_->input_uring_error_bytes_total.fetch_add(requested_bytes,
                                                    std::memory_order_relaxed);
  }
}

void CompactionIOExperiment::JobContext::RecordInputSyncFallback(
    uint64_t requested_bytes) {
  if (!state_->witness_enabled) {
    return;
  }
  std::lock_guard<std::mutex> lock(state_->input_uring_witness_mutex);
  state_->input_sync_fallback_count_total.fetch_add(1,
                                                    std::memory_order_relaxed);
  state_->input_sync_fallback_bytes_total.fetch_add(requested_bytes,
                                                    std::memory_order_relaxed);
}

void CompactionIOExperiment::JobContext::RecordConsumerDemand(
    uint64_t requested_bytes) {
  if (state_->telemetry_enabled) {
    state_->demand_count.fetch_add(1, std::memory_order_relaxed);
    state_->demand_bytes.fetch_add(requested_bytes, std::memory_order_relaxed);
  }
}

void CompactionIOExperiment::JobContext::RecordConsumerPoll(
    uint64_t issue_micros, uint64_t demand_micros, uint64_t completion_micros,
    uint64_t poll_end_micros, bool was_waiting) {
  if (!state_->telemetry_enabled) {
    return;
  }
  const bool causal_timing =
      issue_micros <= demand_micros && issue_micros <= completion_micros &&
      demand_micros <= poll_end_micros && completion_micros <= poll_end_micros;
  if (causal_timing) {
    const uint64_t overlap_end = std::min(demand_micros, completion_micros);
    if (overlap_end > issue_micros) {
      const uint64_t overlap_micros = overlap_end - issue_micros;
      state_->overlap_micros.Add(overlap_micros);
      state_->overlap_total_micros.fetch_add(overlap_micros,
                                             std::memory_order_relaxed);
    }
    if (was_waiting && poll_end_micros > demand_micros) {
      const uint64_t exposed_io_micros = poll_end_micros - demand_micros;
      state_->exposed_io_micros.Add(exposed_io_micros);
      state_->exposed_io_total_micros.fetch_add(exposed_io_micros,
                                                std::memory_order_relaxed);
      state_->read_block_count_total.fetch_add(1, std::memory_order_relaxed);
      state_->read_blocked_us_total.fetch_add(exposed_io_micros,
                                              std::memory_order_relaxed);
      read_block_count_.fetch_add(1, std::memory_order_relaxed);
      read_blocked_us_.fetch_add(exposed_io_micros, std::memory_order_relaxed);
    }
  }
  if (was_waiting) {
    state_->stall_count.fetch_add(1, std::memory_order_relaxed);
  }
}

void CompactionIOExperiment::JobContext::RecordConsumed(
    uint64_t consumed_bytes) {
  if (state_->telemetry_enabled) {
    state_->consumed_bytes.fetch_add(consumed_bytes, std::memory_order_relaxed);
  }
}

void CompactionIOExperiment::JobContext::RecordCompactionOutputWrite(
    uint64_t blocked_micros) {
  if (!state_->telemetry_enabled) {
    return;
  }
  state_->write_blocked_us_total.fetch_add(blocked_micros,
                                           std::memory_order_relaxed);
  state_->write_block_count_total.fetch_add(1, std::memory_order_relaxed);
  write_blocked_us_.fetch_add(blocked_micros, std::memory_order_relaxed);
  write_block_count_.fetch_add(1, std::memory_order_relaxed);
}

void CompactionIOExperiment::JobContext::RecordOutputSync(
    uint64_t sync_micros) {
  if (!state_->telemetry_enabled) {
    return;
  }
  state_->output_sync_us_total.fetch_add(sync_micros,
                                         std::memory_order_relaxed);
  state_->output_sync_count_total.fetch_add(1, std::memory_order_relaxed);
  output_sync_us_.fetch_add(sync_micros, std::memory_order_relaxed);
}

CompactionIOExperiment::JobContextScope::JobContextScope(
    const JobContextPtr& context)
    : previous_(current_job_context_) {
  current_job_context_ = context;
}

CompactionIOExperiment::JobContextScope::~JobContextScope() {
  current_job_context_ = std::move(previous_);
}

CompactionIOExperiment::CompactionIOExperiment(size_t depth, SystemClock* clock,
                                               bool telemetry_enabled,
                                               std::string jsonl_path,
                                               bool witness_enabled)
    : depth_(depth),
      clock_(clock),
      telemetry_enabled_(telemetry_enabled),
      witness_enabled_(witness_enabled),
      jsonl_path_(std::move(jsonl_path)),
      state_(std::make_shared<SharedState>(depth, clock, telemetry_enabled,
                                           witness_enabled)),
      legacy_context_(std::make_shared<JobContext>(state_)) {
  assert(IsSupportedDepth(depth));
  assert(clock_ != nullptr);
}

CompactionIOExperiment::~CompactionIOExperiment() = default;

bool CompactionIOExperiment::IsSupportedDepth(uint64_t depth) {
  return depth == 1 || depth == 2 || depth == 4 || depth == 8 || depth == 16 ||
         depth == 32 || depth == 64;
}

std::shared_ptr<CompactionIOExperiment> CompactionIOExperiment::Active() {
  std::lock_guard<std::mutex> lock(CompactionIOExperimentOwnerMutex());
  return CompactionIOExperimentOwner();
}

CompactionIOExperiment::JobContextPtr
CompactionIOExperiment::CurrentJobContext() {
  return current_job_context_;
}

CompactionIOExperiment::JobContextPtr
CompactionIOExperiment::CurrentOrActiveJobContext() {
  if (current_job_context_ != nullptr) {
    return current_job_context_;
  }
  std::shared_ptr<CompactionIOExperiment> experiment = Active();
  return experiment == nullptr ? nullptr : experiment->legacy_context_;
}

bool CompactionIOExperiment::telemetry_enabled() const {
  return telemetry_enabled_;
}

bool CompactionIOExperiment::witness_enabled() const {
  return witness_enabled_;
}

size_t CompactionIOExperiment::depth() const { return depth_; }

uint64_t CompactionIOExperiment::NowMicros() const {
  return telemetry_enabled_ ? clock_->NowMicros() : 0;
}

uint64_t CompactionIOExperiment::NowNanos() const {
  return telemetry_enabled_ ? clock_->NowNanos() : 0;
}

CompactionIOExperiment::JobContextPtr CompactionIOExperiment::ContextForRecord()
    const {
  if (current_job_context_ != nullptr &&
      current_job_context_->state_ == state_) {
    return current_job_context_;
  }
  return legacy_context_;
}

void CompactionIOExperiment::RecordSynchronousRead(uint64_t requested_bytes,
                                                   uint64_t ready_bytes,
                                                   uint64_t service_micros) {
  ContextForRecord()->RecordSynchronousRead(requested_bytes, ready_bytes,
                                            service_micros);
}

void CompactionIOExperiment::RecordAsynchronousReadIssued(
    uint64_t requested_bytes) {
  ContextForRecord()->RecordAsynchronousReadIssued(requested_bytes);
}

void CompactionIOExperiment::RecordAsynchronousReadRejected(
    uint64_t requested_bytes) {
  ContextForRecord()->RecordAsynchronousReadRejected(requested_bytes);
}

void CompactionIOExperiment::RecordInputAsyncRejected(uint64_t requested_bytes,
                                                      bool not_supported) {
  ContextForRecord()->RecordInputAsyncRejected(requested_bytes, not_supported);
}

void CompactionIOExperiment::RecordAsynchronousReadCompleted(
    uint64_t ready_bytes, uint64_t service_micros) {
  ContextForRecord()->RecordAsynchronousReadCompleted(ready_bytes,
                                                      service_micros);
}

void CompactionIOExperiment::RecordAsynchronousReadAborted() {
  ContextForRecord()->RecordAsynchronousReadAborted();
}

void CompactionIOExperiment::RecordConsumerDemand(uint64_t requested_bytes) {
  ContextForRecord()->RecordConsumerDemand(requested_bytes);
}

void CompactionIOExperiment::RecordConsumerPoll(uint64_t issue_micros,
                                                uint64_t demand_micros,
                                                uint64_t completion_micros,
                                                uint64_t poll_end_micros,
                                                bool was_waiting) {
  ContextForRecord()->RecordConsumerPoll(issue_micros, demand_micros,
                                         completion_micros, poll_end_micros,
                                         was_waiting);
}

void CompactionIOExperiment::RecordConsumed(uint64_t consumed_bytes) {
  ContextForRecord()->RecordConsumed(consumed_bytes);
}

void CompactionIOExperiment::RecordCompactionOutputWrite(
    uint64_t blocked_micros) {
  ContextForRecord()->RecordCompactionOutputWrite(blocked_micros);
}

void CompactionIOExperiment::RecordOutputSync(uint64_t sync_micros) {
  ContextForRecord()->RecordOutputSync(sync_micros);
}

void CompactionIOExperiment::RecordCompactionInterval(
    uint64_t compaction_time_micros, uint64_t cpu_time_micros) {
  if (!telemetry_enabled_) {
    return;
  }
  state_->compaction_time_micros.fetch_add(compaction_time_micros,
                                           std::memory_order_relaxed);
  state_->cpu_time_micros.fetch_add(cpu_time_micros, std::memory_order_relaxed);
}

namespace {

bool ParseDepth(const char* value, uint64_t* depth) {
  if (value == nullptr || value[0] == '\0') {
    return false;
  }
  for (const char* cursor = value; *cursor != '\0'; ++cursor) {
    if (std::isdigit(static_cast<unsigned char>(*cursor)) == 0) {
      return false;
    }
  }
  errno = 0;
  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(value, &end, 10);
  if (errno == ERANGE || end == value || *end != '\0') {
    return false;
  }
  *depth = static_cast<uint64_t>(parsed);
  return true;
}

std::string JsonString(const std::string& value) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string result;
  result.reserve(value.size() + 2);
  result.push_back('"');
  for (const unsigned char character : value) {
    switch (character) {
      case '"':
        result.append("\\\"");
        break;
      case '\\':
        result.append("\\\\");
        break;
      case '\b':
        result.append("\\b");
        break;
      case '\f':
        result.append("\\f");
        break;
      case '\n':
        result.append("\\n");
        break;
      case '\r':
        result.append("\\r");
        break;
      case '\t':
        result.append("\\t");
        break;
      default:
        if (character < 0x20) {
          result.append("\\u00");
          result.push_back(kHex[character >> 4]);
          result.push_back(kHex[character & 0x0f]);
        } else {
          result.push_back(static_cast<char>(character));
        }
    }
  }
  result.push_back('"');
  return result;
}

std::string HistogramToJson(const HistogramImpl& histogram) {
  HistogramData data;
  histogram.Data(&data);
  char buffer[512];
  snprintf(buffer, sizeof(buffer),
           "{\"count\":%" PRIu64
           ",\"avg\":%.6f,\"stddev\":%.6f,"
           "\"p50\":%.6f,\"p95\":%.6f,\"p99\":%.6f,\"max\":%.6f}",
           data.count, data.average, data.standard_deviation, data.median,
           data.percentile95, data.percentile99, data.max);
  return buffer;
}

}  // namespace

Status AcquireCompactionIOExperiment(uint64_t depth, bool telemetry_enabled) {
  if (!CompactionIOExperiment::IsSupportedDepth(depth)) {
    return Status::InvalidArgument(
        "unsupported compaction I/O experiment depth");
  }
  return PublishCompactionIOExperiment(std::make_shared<CompactionIOExperiment>(
      depth, SystemClock::Default().get(), telemetry_enabled));
}

Status AcquireCompactionIOExperimentFromEnvironment() {
  const char* depth_value =
      std::getenv("ROCKSDB_EXPERIMENTAL_COMPACTION_IO_DEPTH");
  if (depth_value == nullptr || depth_value[0] == '\0') {
    return Status::OK();
  }
  uint64_t depth = 0;
  if (!ParseDepth(depth_value, &depth) ||
      !CompactionIOExperiment::IsSupportedDepth(depth)) {
    return Status::InvalidArgument(
        "ROCKSDB_EXPERIMENTAL_COMPACTION_IO_DEPTH must be a supported depth");
  }
  const char* jsonl_path =
      std::getenv("ROCKSDB_EXPERIMENTAL_COMPACTION_IO_JSONL");
  const bool telemetry_enabled = jsonl_path != nullptr && jsonl_path[0] != '\0';
  const char* witness_value =
      std::getenv("ROCKSDB_EXPERIMENTAL_COMPACTION_IO_WITNESS");
  const bool witness_enabled =
      witness_value != nullptr && std::string(witness_value) == "1";
  return PublishCompactionIOExperiment(std::make_shared<CompactionIOExperiment>(
      depth, SystemClock::Default().get(), telemetry_enabled,
      telemetry_enabled ? jsonl_path : std::string(), witness_enabled));
}

Status ReleaseCompactionIOExperiment() {
  std::lock_guard<std::mutex> lock(CompactionIOExperimentOwnerMutex());
  std::shared_ptr<CompactionIOExperiment>& owner =
      CompactionIOExperimentOwner();
  if (owner == nullptr) {
    return Status::OK();
  }
  if (owner.use_count() != 1) {
    return Status::Busy(
        "cannot release compaction I/O experiment while borrowers remain");
  }
  {
    std::lock_guard<std::mutex> jobs_lock(owner->active_jobs_mutex_);
    if (owner->active_compactions_ != 0 ||
        owner->outstanding_async_reads() != 0) {
      return Status::Busy(
          "cannot release compaction I/O experiment while work is active");
    }
  }
  Status status = owner->WriteJobRecordsToJsonl();
  owner.reset();
  return status;
}

CompactionIOExperiment::JobContextPtr
CompactionIOExperiment::BeginCompactionJob(
    uint64_t job_id, uint64_t start_monotonic_us, uint64_t start_epoch_us,
    uint64_t thread_id, int input_level, int output_level,
    const std::string& compaction_reason) {
  if (!telemetry_enabled_) {
    return nullptr;
  }
  ActiveJob active_job;
  active_job.context = std::make_shared<JobContext>(state_, thread_id);
  active_job.metadata.thread_id = thread_id;
  active_job.metadata.job_id = job_id;
  active_job.metadata.start_monotonic_us = start_monotonic_us;
  active_job.metadata.start_epoch_us = start_epoch_us;
  active_job.metadata.compaction_reason = compaction_reason;
  active_job.metadata.input_level = input_level;
  active_job.metadata.output_level = output_level;
  std::lock_guard<std::mutex> lock(active_jobs_mutex_);
  const auto [it, inserted] =
      active_jobs_.emplace(job_id, std::move(active_job));
  if (!inserted) {
    return nullptr;
  }
  AdvanceWorkerTimeLocked(start_monotonic_us);
  ++active_compactions_;
  ++compaction_lifecycle_count_;
  return it->second.context;
}

void CompactionIOExperiment::CompleteCompactionJob(
    uint64_t job_id, uint64_t end_monotonic_us, uint64_t end_epoch_us,
    uint64_t compaction_wall_us, uint64_t compaction_cpu_us,
    uint64_t logical_input_bytes, uint64_t logical_output_bytes,
    bool succeeded) {
  if (!telemetry_enabled_) {
    return;
  }
  JobRecord record;
  {
    std::lock_guard<std::mutex> lock(active_jobs_mutex_);
    const auto it = active_jobs_.find(job_id);
    if (it == active_jobs_.end()) {
      return;
    }
    AdvanceWorkerTimeLocked(end_monotonic_us);
    assert(active_compactions_ > 0);
    --active_compactions_;
    record = it->second.metadata;
    record.context = it->second.context;
    active_jobs_.erase(it);
  }
  record.end_monotonic_us = end_monotonic_us;
  record.end_epoch_us = end_epoch_us;
  record.compaction_wall_us = compaction_wall_us;
  record.compaction_cpu_us = compaction_cpu_us;
  record.logical_input_bytes = logical_input_bytes;
  record.logical_output_bytes = logical_output_bytes;
  if (succeeded) {
    state_->logical_input_bytes_total.fetch_add(logical_input_bytes,
                                                std::memory_order_relaxed);
    state_->logical_output_bytes_total.fetch_add(logical_output_bytes,
                                                 std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(job_records_mutex_);
    job_records_.emplace_back(std::move(record));
  }
}

void CompactionIOExperiment::AdvanceWorkerTimeLocked(uint64_t monotonic_us) {
  monotonic_us = std::max(monotonic_us, last_transition_monotonic_us_);
  if (active_compactions_ > 0 && monotonic_us > last_transition_monotonic_us_) {
    accumulated_compaction_worker_us_ +=
        active_compactions_ * (monotonic_us - last_transition_monotonic_us_);
  }
  last_transition_monotonic_us_ = monotonic_us;
}

CompactionIOExperiment::Snapshot CompactionIOExperiment::GetSnapshotAtLocked(
    uint64_t monotonic_us) const {
  Snapshot snapshot;
  snapshot.read_blocked_us_total =
      state_->read_blocked_us_total.load(std::memory_order_relaxed);
  snapshot.read_block_count_total =
      state_->read_block_count_total.load(std::memory_order_relaxed);
  snapshot.write_blocked_us_total =
      state_->write_blocked_us_total.load(std::memory_order_relaxed);
  snapshot.write_block_count_total =
      state_->write_block_count_total.load(std::memory_order_relaxed);
  snapshot.output_sync_us_total =
      state_->output_sync_us_total.load(std::memory_order_relaxed);
  snapshot.output_sync_count_total =
      state_->output_sync_count_total.load(std::memory_order_relaxed);
  snapshot.compaction_worker_us_total = accumulated_compaction_worker_us_;
  snapshot.logical_input_bytes_total =
      state_->logical_input_bytes_total.load(std::memory_order_relaxed);
  snapshot.logical_output_bytes_total =
      state_->logical_output_bytes_total.load(std::memory_order_relaxed);
  if (active_compactions_ > 0 && monotonic_us > last_transition_monotonic_us_) {
    snapshot.compaction_worker_us_total +=
        active_compactions_ * (monotonic_us - last_transition_monotonic_us_);
  }
  snapshot.active_compactions = active_compactions_;
  snapshot.compaction_lifecycle_count = compaction_lifecycle_count_;
  snapshot.witness_collected = witness_enabled_;
  if (!witness_enabled_) {
    return snapshot;
  }
  std::lock_guard<std::mutex> witness_lock(state_->input_uring_witness_mutex);
  snapshot.input_uring_submit_count_total =
      state_->input_uring_submit_count_total.load(std::memory_order_relaxed);
  snapshot.input_uring_submit_bytes_total =
      state_->input_uring_submit_bytes_total.load(std::memory_order_relaxed);
  snapshot.input_uring_complete_count_total =
      state_->input_uring_complete_count_total.load(std::memory_order_relaxed);
  snapshot.input_uring_complete_bytes_total =
      state_->input_uring_complete_bytes_total.load(std::memory_order_relaxed);
  snapshot.input_uring_complete_request_bytes_total =
      state_->input_uring_complete_request_bytes_total.load(
          std::memory_order_relaxed);
  snapshot.input_uring_zero_byte_completion_count_total =
      state_->input_uring_zero_byte_completion_count_total.load(
          std::memory_order_relaxed);
  snapshot.input_uring_zero_byte_request_bytes_total =
      state_->input_uring_zero_byte_request_bytes_total.load(
          std::memory_order_relaxed);
  snapshot.input_uring_rejected_count_total =
      state_->input_uring_rejected_count_total.load(std::memory_order_relaxed);
  snapshot.input_uring_rejected_bytes_total =
      state_->input_uring_rejected_bytes_total.load(std::memory_order_relaxed);
  snapshot.input_uring_not_supported_count_total =
      state_->input_uring_not_supported_count_total.load(
          std::memory_order_relaxed);
  snapshot.input_sync_fallback_count_total =
      state_->input_sync_fallback_count_total.load(std::memory_order_relaxed);
  snapshot.input_sync_fallback_bytes_total =
      state_->input_sync_fallback_bytes_total.load(std::memory_order_relaxed);
  snapshot.input_uring_abort_count_total =
      state_->input_uring_abort_count_total.load(std::memory_order_relaxed);
  snapshot.input_uring_abort_bytes_total =
      state_->input_uring_abort_bytes_total.load(std::memory_order_relaxed);
  snapshot.input_uring_error_count_total =
      state_->input_uring_error_count_total.load(std::memory_order_relaxed);
  snapshot.input_uring_error_bytes_total =
      state_->input_uring_error_bytes_total.load(std::memory_order_relaxed);
  snapshot.input_uring_outstanding_count =
      state_->input_uring_outstanding_count.load(std::memory_order_relaxed);
  snapshot.input_uring_outstanding_bytes =
      state_->input_uring_outstanding_bytes.load(std::memory_order_relaxed);
  snapshot.effective_prefetch_depth =
      state_->effective_prefetch_depth.load(std::memory_order_relaxed);
  snapshot.worker_thread_id =
      state_->worker_thread_id.load(std::memory_order_relaxed);
  snapshot.worker_ring_ready =
      state_->worker_ring_ready.load(std::memory_order_relaxed);
  return snapshot;
}

CompactionIOExperiment::Snapshot CompactionIOExperiment::GetSnapshot() const {
  if (!telemetry_enabled_) {
    return {};
  }
  std::lock_guard<std::mutex> lock(active_jobs_mutex_);
  const uint64_t monotonic_us = active_compactions_ > 0
                                    ? clock_->NowNanos() / 1000
                                    : last_transition_monotonic_us_;
  return GetSnapshotAtLocked(monotonic_us);
}

CompactionIOExperiment::Snapshot CompactionIOExperiment::GetSnapshotAt(
    uint64_t monotonic_us) const {
  if (!telemetry_enabled_) {
    return {};
  }
  std::lock_guard<std::mutex> lock(active_jobs_mutex_);
  return GetSnapshotAtLocked(monotonic_us);
}

bool CompactionIOExperiment::RecordRuntimeMaxSubcompactions(
    uint32_t max_subcompactions) {
  if (!telemetry_enabled_) {
    return true;
  }
  std::lock_guard<std::mutex> lock(active_jobs_mutex_);
  runtime_max_subcompactions_ =
      std::max(runtime_max_subcompactions_, max_subcompactions);
  max_subcompactions_valid_ =
      max_subcompactions_valid_ && max_subcompactions == 1;
  return max_subcompactions_valid_;
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

Status CompactionIOExperiment::WriteJobRecordsToJsonl() const {
  if (jsonl_path_.empty()) {
    return Status::OK();
  }
  std::ofstream output(jsonl_path_, std::ios::out | std::ios::trunc);
  if (!output.is_open()) {
    return Status::IOError("failed to open compaction I/O JSONL output: " +
                           jsonl_path_);
  }
  const std::string records = JobRecordsToJsonLines();
  output.write(records.data(), static_cast<std::streamsize>(records.size()));
  if (!output.good()) {
    return Status::IOError("failed to write compaction I/O JSONL output: " +
                           jsonl_path_);
  }
  output.flush();
  if (!output.good()) {
    return Status::IOError("failed to flush compaction I/O JSONL output: " +
                           jsonl_path_);
  }
  output.close();
  if (output.fail()) {
    return Status::IOError("failed to close compaction I/O JSONL output: " +
                           jsonl_path_);
  }
  return Status::OK();
}

std::string CompactionIOExperiment::ToJson() const {
  char buffer[3200];
  const Snapshot snapshot = GetSnapshot();
  uint32_t runtime_max_subcompactions;
  bool max_subcompactions_valid;
  {
    std::lock_guard<std::mutex> lock(active_jobs_mutex_);
    runtime_max_subcompactions = runtime_max_subcompactions_;
    max_subcompactions_valid = max_subcompactions_valid_;
  }
  size_t completed_job_count;
  {
    std::lock_guard<std::mutex> lock(job_records_mutex_);
    completed_job_count = job_records_.size();
  }
  snprintf(
      buffer, sizeof(buffer),
      "{\"compaction_io_experiment\":{\"depth\":%zu,"
      "\"request_span_bytes\":%zu,\"t_compaction_us\":%" PRIu64
      ","
      "\"cpu_time_us\":%" PRIu64 ",\"read_count\":%" PRIu64
      ","
      "\"read_bytes\":%" PRIu64 ",\"prefetched_bytes\":%" PRIu64
      ",\"ready_bytes\":%" PRIu64 ",\"consumed_bytes\":%" PRIu64
      ",\"demand_count\":%" PRIu64 ",\"demand_bytes\":%" PRIu64
      ",\"stall_count\":%" PRIu64 ",\"outstanding_async_reads\":%" PRIu64
      ",\"overlap_total_us\":%" PRIu64 ",\"read_blocked_us_total\":%" PRIu64
      ",\"read_block_count_total\":%" PRIu64
      ",\"write_blocked_us_total\":%" PRIu64
      ",\"write_block_count_total\":%" PRIu64
      ",\"output_sync_us_total\":%" PRIu64
      ",\"output_sync_count_total\":%" PRIu64
      ",\"compaction_worker_us_total\":%" PRIu64
      ",\"logical_input_bytes_total\":%" PRIu64
      ",\"logical_output_bytes_total\":%" PRIu64
      ",\"active_compactions\":%" PRIu64
      ",\"compaction_lifecycle_count\":%" PRIu64
      ",\"exposed_io_total_us\":%" PRIu64 ",\"read_block_count\":%" PRIu64
      ",\"write_blocked_us\":%" PRIu64 ",\"write_block_count\":%" PRIu64
      ",\"output_sync_us\":%" PRIu64 ",\"runtime_max_subcompactions\":%" PRIu32
      ",\"max_subcompactions_valid\":%s"
      ",\"completed_compaction_jobs\":%zu,"
      "\"io_service_us\":%s,\"sync_fallback_read_us\":%s,"
      "\"overlap_us\":%s,\"exposed_io_us\":%s}}",
      depth_, kRequestSpan,
      state_->compaction_time_micros.load(std::memory_order_relaxed),
      state_->cpu_time_micros.load(std::memory_order_relaxed),
      state_->read_count.load(std::memory_order_relaxed),
      state_->read_bytes.load(std::memory_order_relaxed),
      state_->prefetched_bytes.load(std::memory_order_relaxed),
      state_->ready_bytes.load(std::memory_order_relaxed),
      state_->consumed_bytes.load(std::memory_order_relaxed),
      state_->demand_count.load(std::memory_order_relaxed),
      state_->demand_bytes.load(std::memory_order_relaxed),
      state_->stall_count.load(std::memory_order_relaxed),
      state_->outstanding_async_reads.load(std::memory_order_relaxed),
      state_->overlap_total_micros.load(std::memory_order_relaxed),
      snapshot.read_blocked_us_total, snapshot.read_block_count_total,
      snapshot.write_blocked_us_total, snapshot.write_block_count_total,
      snapshot.output_sync_us_total, snapshot.output_sync_count_total,
      snapshot.compaction_worker_us_total, snapshot.logical_input_bytes_total,
      snapshot.logical_output_bytes_total, snapshot.active_compactions,
      snapshot.compaction_lifecycle_count,
      state_->exposed_io_total_micros.load(std::memory_order_relaxed),
      snapshot.read_block_count_total, snapshot.write_blocked_us_total,
      snapshot.write_block_count_total, snapshot.output_sync_us_total,
      runtime_max_subcompactions, max_subcompactions_valid ? "true" : "false",
      completed_job_count, HistogramToJson(state_->io_service_micros).c_str(),
      HistogramToJson(state_->sync_fallback_read_micros).c_str(),
      HistogramToJson(state_->overlap_micros).c_str(),
      HistogramToJson(state_->exposed_io_micros).c_str());
  return buffer;
}

uint64_t CompactionIOExperiment::outstanding_async_reads() const {
  return state_->outstanding_async_reads.load(std::memory_order_relaxed);
}

std::string CompactionIOExperiment::JobRecord::ToJson() const {
  const uint64_t record_read_blocked_us =
      context == nullptr ? read_blocked_us : context->read_blocked_us();
  const uint64_t record_read_block_count =
      context == nullptr ? read_block_count : context->read_block_count();
  const uint64_t record_write_blocked_us =
      context == nullptr ? write_blocked_us : context->write_blocked_us();
  const uint64_t record_write_block_count =
      context == nullptr ? write_block_count : context->write_block_count();
  const uint64_t record_output_sync_us =
      context == nullptr ? output_sync_us : context->output_sync_us();

  std::string result;
  result.reserve(512 + compaction_reason.size());
  result.append("{\"thread_id\":");
  result.append(std::to_string(thread_id));
  result.append(",\"job_id\":");
  result.append(std::to_string(job_id));
  result.append(",\"start_monotonic_us\":");
  result.append(std::to_string(start_monotonic_us));
  result.append(",\"start_epoch_us\":");
  result.append(std::to_string(start_epoch_us));
  result.append(",\"end_monotonic_us\":");
  result.append(std::to_string(end_monotonic_us));
  result.append(",\"end_epoch_us\":");
  result.append(std::to_string(end_epoch_us));
  result.append(",\"compaction_wall_us\":");
  result.append(std::to_string(compaction_wall_us));
  result.append(",\"logical_input_bytes\":");
  result.append(std::to_string(logical_input_bytes));
  result.append(",\"compaction_reason\":");
  result.append(JsonString(compaction_reason));
  result.append(",\"input_level\":");
  result.append(std::to_string(input_level));
  result.append(",\"output_level\":");
  result.append(std::to_string(output_level));
  result.append(",\"compaction_cpu_us\":");
  result.append(std::to_string(compaction_cpu_us));
  result.append(",\"logical_output_bytes\":");
  result.append(std::to_string(logical_output_bytes));
  result.append(",\"read_blocked_us\":");
  result.append(std::to_string(record_read_blocked_us));
  result.append(",\"read_block_count\":");
  result.append(std::to_string(record_read_block_count));
  result.append(",\"write_blocked_us\":");
  result.append(std::to_string(record_write_blocked_us));
  result.append(",\"write_block_count\":");
  result.append(std::to_string(record_write_block_count));
  result.append(",\"output_sync_us\":");
  result.append(std::to_string(record_output_sync_us));
  result.append(",\"status\":");
  result.append(JsonString(status));
  result.push_back('}');
  return result;
}

}  // namespace ROCKSDB_NAMESPACE
