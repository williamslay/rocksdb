//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <mutex>
#include <string>
#include <thread>

#include "file/compaction_io_experiment.h"

namespace {

constexpr uint64_t kSnapshotSchemaVersion = 5;
constexpr char kProtocolVersion[] = "R3-v5";

struct SnapshotSamplerState {
  std::condition_variable condition;
  std::mutex mutex;
  std::thread thread;
  bool running = false;
  bool stop_requested = false;
  std::string path;
  std::string run_id;
  std::string error;
  std::shared_ptr<ROCKSDB_NAMESPACE::CompactionIOExperiment> experiment;
  uint64_t last_monotonic_ns = 0;
  uint64_t last_epoch_ns = 0;
};

SnapshotSamplerState& GetSnapshotSamplerState() {
  static SnapshotSamplerState* state = new SnapshotSamplerState();
  return *state;
}

std::string JsonString(const std::string& value) {
  std::string result;
  result.reserve(value.size() + 2);
  result.push_back('"');
  for (const unsigned char character : value) {
    if (character == '"' || character == '\\') {
      result.push_back('\\');
      result.push_back(static_cast<char>(character));
    } else if (character < 0x20) {
      result.append("\\u00");
      static constexpr char kHex[] = "0123456789abcdef";
      result.push_back(kHex[character >> 4]);
      result.push_back(kHex[character & 0x0f]);
    } else {
      result.push_back(static_cast<char>(character));
    }
  }
  result.push_back('"');
  return result;
}

std::string RunIdFromEnvironment() {
  const char* run_id = std::getenv("R3_RUN_ID");
  return run_id == nullptr ? std::string() : std::string(run_id);
}

uint64_t StrictlyIncreasing(uint64_t value, uint64_t previous) {
  if (value > previous) {
    return value;
  }
  return previous == std::numeric_limits<uint64_t>::max() ? previous
                                                          : previous + 1;
}

ROCKSDB_NAMESPACE::Status WriteSnapshotLineLocked(
    SnapshotSamplerState* state, const std::string& path,
    ROCKSDB_NAMESPACE::CompactionIOExperiment* experiment,
    const std::string& run_id, bool truncate) {
  const uint64_t timestamp_monotonic_ns =
      StrictlyIncreasing(experiment->NowNanos(), state->last_monotonic_ns);
  const uint64_t epoch_micros = experiment->NowMicros();
  const uint64_t raw_epoch_ns =
      epoch_micros > std::numeric_limits<uint64_t>::max() / 1000
          ? std::numeric_limits<uint64_t>::max()
          : epoch_micros * 1000;
  const uint64_t timestamp_epoch_ns =
      StrictlyIncreasing(raw_epoch_ns, state->last_epoch_ns);
  const ROCKSDB_NAMESPACE::CompactionIOExperiment::Snapshot snapshot =
      experiment->GetSnapshotAt(timestamp_monotonic_ns / 1000);

  const std::ios_base::openmode mode =
      std::ios::out | (truncate ? std::ios::trunc : std::ios::app);
  std::ofstream output(path, mode);
  if (!output.is_open()) {
    return ROCKSDB_NAMESPACE::Status::IOError(
        "failed to open compaction I/O snapshot output: " + path);
  }

  std::string line;
  line.reserve(512 + run_id.size());
  line.append("{\"schema_version\":");
  line.append(std::to_string(kSnapshotSchemaVersion));
  line.append(",\"protocol_version\":");
  line.append(JsonString(kProtocolVersion));
  line.append(",\"run_id\":");
  line.append(JsonString(run_id));
  line.append(",\"timestamp_monotonic_ns\":");
  line.append(std::to_string(timestamp_monotonic_ns));
  line.append(",\"timestamp_epoch_ns\":");
  line.append(std::to_string(timestamp_epoch_ns));
  line.append(",\"read_blocked_us_total\":");
  line.append(std::to_string(snapshot.read_blocked_us_total));
  line.append(",\"read_block_count_total\":");
  line.append(std::to_string(snapshot.read_block_count_total));
  line.append(",\"write_blocked_us_total\":");
  line.append(std::to_string(snapshot.write_blocked_us_total));
  line.append(",\"write_block_count_total\":");
  line.append(std::to_string(snapshot.write_block_count_total));
  line.append(",\"output_sync_us_total\":");
  line.append(std::to_string(snapshot.output_sync_us_total));
  line.append(",\"output_sync_count_total\":");
  line.append(std::to_string(snapshot.output_sync_count_total));
  line.append(",\"compaction_worker_us_total\":");
  line.append(std::to_string(snapshot.compaction_worker_us_total));
  line.append(",\"logical_input_bytes_total\":");
  line.append(std::to_string(snapshot.logical_input_bytes_total));
  line.append(",\"logical_output_bytes_total\":");
  line.append(std::to_string(snapshot.logical_output_bytes_total));
  line.append(",\"active_compactions\":");
  line.append(std::to_string(snapshot.active_compactions));
  line.append(",\"compaction_lifecycle_count\":");
  line.append(std::to_string(snapshot.compaction_lifecycle_count));
  if (snapshot.witness_collected) {
    line.append(",\"effective_prefetch_depth\":");
    line.append(std::to_string(snapshot.effective_prefetch_depth));
    line.append(",\"worker_thread_id\":");
    line.append(std::to_string(snapshot.worker_thread_id));
    line.append(",\"worker_ring_ready\":");
    line.append(snapshot.worker_ring_ready ? "true" : "false");
    line.append(",\"input_uring_submit_count_total\":");
    line.append(std::to_string(snapshot.input_uring_submit_count_total));
    line.append(",\"input_uring_submit_bytes_total\":");
    line.append(std::to_string(snapshot.input_uring_submit_bytes_total));
    line.append(",\"input_uring_complete_count_total\":");
    line.append(std::to_string(snapshot.input_uring_complete_count_total));
    line.append(",\"input_uring_complete_bytes_total\":");
    line.append(std::to_string(snapshot.input_uring_complete_bytes_total));
    line.append(",\"input_uring_complete_request_bytes_total\":");
    line.append(
        std::to_string(snapshot.input_uring_complete_request_bytes_total));
    line.append(",\"input_uring_zero_byte_completion_count_total\":");
    line.append(
        std::to_string(snapshot.input_uring_zero_byte_completion_count_total));
    line.append(",\"input_uring_zero_byte_request_bytes_total\":");
    line.append(
        std::to_string(snapshot.input_uring_zero_byte_request_bytes_total));
    line.append(",\"input_uring_rejected_count_total\":");
    line.append(std::to_string(snapshot.input_uring_rejected_count_total));
    line.append(",\"input_uring_rejected_bytes_total\":");
    line.append(std::to_string(snapshot.input_uring_rejected_bytes_total));
    line.append(",\"input_uring_not_supported_count_total\":");
    line.append(std::to_string(snapshot.input_uring_not_supported_count_total));
    line.append(",\"input_sync_fallback_count_total\":");
    line.append(std::to_string(snapshot.input_sync_fallback_count_total));
    line.append(",\"input_sync_fallback_bytes_total\":");
    line.append(std::to_string(snapshot.input_sync_fallback_bytes_total));
    line.append(",\"input_uring_abort_count_total\":");
    line.append(std::to_string(snapshot.input_uring_abort_count_total));
    line.append(",\"input_uring_abort_bytes_total\":");
    line.append(std::to_string(snapshot.input_uring_abort_bytes_total));
    line.append(",\"input_uring_error_count_total\":");
    line.append(std::to_string(snapshot.input_uring_error_count_total));
    line.append(",\"input_uring_error_bytes_total\":");
    line.append(std::to_string(snapshot.input_uring_error_bytes_total));
    line.append(",\"input_uring_outstanding_count\":");
    line.append(std::to_string(snapshot.input_uring_outstanding_count));
    line.append(",\"input_uring_outstanding_bytes\":");
    line.append(std::to_string(snapshot.input_uring_outstanding_bytes));
  }
  line.push_back('}');
  line.push_back('\n');
  output.write(line.data(), static_cast<std::streamsize>(line.size()));
  if (!output.good()) {
    return ROCKSDB_NAMESPACE::Status::IOError(
        "failed to write compaction I/O snapshot output: " + path);
  }
  output.flush();
  if (!output.good()) {
    return ROCKSDB_NAMESPACE::Status::IOError(
        "failed to flush compaction I/O snapshot output: " + path);
  }
  output.close();
  if (output.fail()) {
    return ROCKSDB_NAMESPACE::Status::IOError(
        "failed to close compaction I/O snapshot output: " + path);
  }
  state->last_monotonic_ns = timestamp_monotonic_ns;
  state->last_epoch_ns = timestamp_epoch_ns;
  return ROCKSDB_NAMESPACE::Status::OK();
}

void SnapshotSamplerLoop(SnapshotSamplerState* state) {
  std::chrono::steady_clock::time_point next_sample =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  for (;;) {
    std::unique_lock<std::mutex> lock(state->mutex);
    if (state->condition.wait_until(
            lock, next_sample, [state] { return state->stop_requested; })) {
      return;
    }
    if (!state->running || state->experiment == nullptr) {
      return;
    }
    const ROCKSDB_NAMESPACE::Status status = WriteSnapshotLineLocked(
        state, state->path, state->experiment.get(), state->run_id, false);
    if (!status.ok()) {
      state->error = status.ToString();
      state->stop_requested = true;
      return;
    }
    next_sample += std::chrono::seconds(1);
    const std::chrono::steady_clock::time_point now =
        std::chrono::steady_clock::now();
    if (next_sample <= now) {
      next_sample = now + std::chrono::seconds(1);
    }
  }
}

ROCKSDB_NAMESPACE::Status StopSnapshotSampler() {
  SnapshotSamplerState& state = GetSnapshotSamplerState();
  std::thread thread;
  {
    std::lock_guard<std::mutex> lock(state.mutex);
    if (!state.thread.joinable()) {
      state.running = false;
      state.experiment.reset();
      return state.error.empty()
                 ? ROCKSDB_NAMESPACE::Status::OK()
                 : ROCKSDB_NAMESPACE::Status::IOError(state.error);
    }
    state.stop_requested = true;
    state.condition.notify_all();
    thread = std::move(state.thread);
  }
  thread.join();
  std::lock_guard<std::mutex> lock(state.mutex);
  state.running = false;
  state.stop_requested = false;
  state.experiment.reset();
  return state.error.empty() ? ROCKSDB_NAMESPACE::Status::OK()
                             : ROCKSDB_NAMESPACE::Status::IOError(state.error);
}

void StopSnapshotSamplerAtExit() { (void)StopSnapshotSampler(); }

void RegisterSnapshotSamplerAtExit() {
  static const bool registered = []() {
    std::atexit(StopSnapshotSamplerAtExit);
    return true;
  }();
  (void)registered;
}

void ResetSnapshotSamplerState() {
  SnapshotSamplerState& state = GetSnapshotSamplerState();
  std::lock_guard<std::mutex> lock(state.mutex);
  state.path.clear();
  state.run_id.clear();
  state.error.clear();
  state.experiment.reset();
  state.last_monotonic_ns = 0;
  state.last_epoch_ns = 0;
}

void ThrowStatus(JNIEnv* env, const ROCKSDB_NAMESPACE::Status& status) {
  if (status.ok()) {
    return;
  }
  jclass exception_class = env->FindClass("java/lang/IllegalStateException");
  if (exception_class != nullptr) {
    const std::string message = status.ToString();
    env->ThrowNew(exception_class, message.c_str());
    env->DeleteLocalRef(exception_class);
  }
}

ROCKSDB_NAMESPACE::Status WriteSnapshot(const std::string& path,
                                        bool truncate) {
  std::shared_ptr<ROCKSDB_NAMESPACE::CompactionIOExperiment> experiment =
      ROCKSDB_NAMESPACE::CompactionIOExperiment::Active();
  if (experiment == nullptr || !experiment->telemetry_enabled()) {
    return ROCKSDB_NAMESPACE::Status::OK();
  }
  if (path.empty()) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "compaction I/O snapshot path is empty");
  }

  if (truncate) {
    const std::string run_id = RunIdFromEnvironment();
    if (run_id.empty()) {
      return ROCKSDB_NAMESPACE::Status::InvalidArgument(
          "R3_RUN_ID is required for compaction I/O snapshots");
    }
    const ROCKSDB_NAMESPACE::Status stop_status = StopSnapshotSampler();
    if (!stop_status.ok()) {
      ResetSnapshotSamplerState();
      return stop_status;
    }
    ResetSnapshotSamplerState();
    RegisterSnapshotSamplerAtExit();
    SnapshotSamplerState& state = GetSnapshotSamplerState();
    ROCKSDB_NAMESPACE::Status status;
    {
      std::lock_guard<std::mutex> lock(state.mutex);
      state.path = path;
      state.run_id = run_id;
      state.experiment = experiment;
      state.running = true;
      status =
          WriteSnapshotLineLocked(&state, path, experiment.get(), run_id, true);
      if (status.ok()) {
        state.thread = std::thread(SnapshotSamplerLoop, &state);
      } else {
        state.running = false;
        state.experiment.reset();
      }
    }
    return status;
  }

  const ROCKSDB_NAMESPACE::Status stop_status = StopSnapshotSampler();
  if (!stop_status.ok()) {
    ResetSnapshotSamplerState();
    return stop_status;
  }
  SnapshotSamplerState& state = GetSnapshotSamplerState();
  ROCKSDB_NAMESPACE::Status status;
  bool missing_run_id = false;
  {
    std::lock_guard<std::mutex> lock(state.mutex);
    const bool same_session = state.path == path && !state.run_id.empty();
    if (!same_session) {
      state.path = path;
      state.run_id = RunIdFromEnvironment();
      if (state.run_id.empty()) {
        missing_run_id = true;
      } else {
        state.last_monotonic_ns = 0;
        state.last_epoch_ns = 0;
      }
    }
    if (!missing_run_id) {
      status = WriteSnapshotLineLocked(&state, path, experiment.get(),
                                       state.run_id, false);
    }
  }
  ResetSnapshotSamplerState();
  if (missing_run_id) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "R3_RUN_ID is required for compaction I/O snapshots");
  }
  return status;
}

}  // namespace

#ifdef OS_LINUX
extern "C" bool RocksDbIOUringEnable() {
  const char* test_disabled = std::getenv("R3_TEST_DISABLE_IO_URING");
  return test_disabled == nullptr || std::string(test_disabled) == "0";
}
#endif

extern "C" JNIEXPORT void JNICALL
Java_site_ycsb_db_rocksdb_R3RocksDBClient_acquireCompactionIOExperiment(
    JNIEnv* env, jclass) {
  const ROCKSDB_NAMESPACE::Status stop_status = StopSnapshotSampler();
  if (!stop_status.ok()) {
    ThrowStatus(env, stop_status);
    return;
  }
  ResetSnapshotSamplerState();
  ThrowStatus(
      env, ROCKSDB_NAMESPACE::AcquireCompactionIOExperimentFromEnvironment());
}

extern "C" JNIEXPORT void JNICALL
Java_site_ycsb_db_rocksdb_R3RocksDBClient_releaseCompactionIOExperiment(
    JNIEnv* env, jclass) {
  const ROCKSDB_NAMESPACE::Status stop_status = StopSnapshotSampler();
  ResetSnapshotSamplerState();
  if (!stop_status.ok()) {
    ThrowStatus(env, stop_status);
    return;
  }
  ThrowStatus(env, ROCKSDB_NAMESPACE::ReleaseCompactionIOExperiment());
}

extern "C" JNIEXPORT void JNICALL
Java_site_ycsb_db_rocksdb_R3RocksDBClient_writeCompactionIOSnapshot(
    JNIEnv* env, jclass, jstring jpath, jboolean jtruncate) {
  if (jpath == nullptr) {
    ThrowStatus(env, ROCKSDB_NAMESPACE::Status::InvalidArgument(
                         "compaction I/O snapshot path is null"));
    return;
  }
  const char* path = env->GetStringUTFChars(jpath, nullptr);
  if (path == nullptr) {
    return;
  }
  const ROCKSDB_NAMESPACE::Status status =
      WriteSnapshot(path, jtruncate == JNI_TRUE);
  env->ReleaseStringUTFChars(jpath, path);
  ThrowStatus(env, status);
}
