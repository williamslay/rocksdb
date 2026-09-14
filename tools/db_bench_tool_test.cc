//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/db_bench_tool.h"

#include "db/db_impl/db_impl.h"
#include "options/options_parser.h"
#include "rocksdb/utilities/options_util.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include <cstdlib>
#include <cstdio>

#ifdef GFLAGS
#include "util/gflags_compat.h"

#include <atomic>
#include <fstream>
#include <iterator>
#ifndef OS_WIN
#include <limits.h>
#endif
#include <memory>
#ifndef OS_WIN
#include <fcntl.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

DECLARE_string(benchmarks);
DECLARE_string(experimental_compaction_input_path);
DECLARE_string(experimental_compaction_output_path);
DECLARE_int32(experimental_compaction_output_path_id);
DECLARE_int64(num);
DECLARE_uint64(experimental_compaction_io_depth);
DECLARE_int32(max_background_compactions);
DECLARE_int32(subcompactions);
DECLARE_bool(mmap_read);
DECLARE_bool(mmap_write);
DECLARE_bool(report_bg_io_stats);
DECLARE_bool(use_direct_io_for_compaction_reads);
DECLARE_bool(use_direct_io_for_flush_and_compaction);
DECLARE_bool(use_direct_reads);

#ifndef OS_WIN
extern char** environ;
#endif

namespace ROCKSDB_NAMESPACE {

namespace {
static const int kMaxArgCount = 100;
static const size_t kArgBufferSize = 100000;
std::string db_bench_path = "./db_bench";

std::string GetDbBenchPath(const char* argv0) {
  const char* injected_path = std::getenv("ROCKSDB_DB_BENCH_PATH");
  if (injected_path != nullptr && injected_path[0] != '\0') {
    return injected_path;
  }
#ifdef ROCKSDB_DB_BENCH_PATH
  return ROCKSDB_DB_BENCH_PATH;
#endif
#ifndef OS_WIN
  char executable_link[PATH_MAX];
  const ssize_t executable_length =
      readlink("/proc/self/exe", executable_link, sizeof(executable_link) - 1);
  const std::string executable_path =
      executable_length > 0
          ? std::string(executable_link, executable_length)
          : std::string(argv0);
  const size_t separator = executable_path.find_last_of('/');
  if (separator != std::string::npos) {
    return executable_path.substr(0, separator) + "/db_bench";
  }
#endif
  return "./db_bench";
}

#ifndef OS_WIN
struct SubprocessResult {
  int exit_status;
  std::string stdout_text;
  std::string stderr_text;
};

SubprocessResult RunDbBenchSubprocess(const std::vector<std::string>& args,
                                      const std::string& test_path) {
  std::vector<std::string> exec_args = args;
  exec_args[0] = db_bench_path;
  std::vector<char*> argv;
  argv.reserve(exec_args.size() + 1);
  for (auto& arg : exec_args) {
    argv.push_back(const_cast<char*>(arg.c_str()));
  }
  argv.push_back(nullptr);

  const std::string stdout_path = test_path + "/subprocess.stdout";
  const std::string stderr_path = test_path + "/subprocess.stderr";
  posix_spawn_file_actions_t file_actions;
  if (posix_spawn_file_actions_init(&file_actions) != 0 ||
      posix_spawn_file_actions_addopen(&file_actions, STDOUT_FILENO,
                                       stdout_path.c_str(),
                                       O_WRONLY | O_CREAT | O_TRUNC, 0644) !=
          0 ||
      posix_spawn_file_actions_addopen(&file_actions, STDERR_FILENO,
                                       stderr_path.c_str(),
                                       O_WRONLY | O_CREAT | O_TRUNC, 0644) !=
          0) {
    ADD_FAILURE() << "Unable to configure db_bench subprocess output";
    return {-1, "", ""};
  }

  pid_t pid = 0;
  const int spawn_status = posix_spawn(
      &pid, argv[0], &file_actions, nullptr, argv.data(), ::environ);
  posix_spawn_file_actions_destroy(&file_actions);
  if (spawn_status != 0) {
    ADD_FAILURE() << "Unable to start db_bench: " << spawn_status;
    return {-1, "", ""};
  }

  int wait_status = 0;
  if (waitpid(pid, &wait_status, 0) != pid) {
    ADD_FAILURE() << "Unable to wait for db_bench subprocess";
    return {-1, "", ""};
  }

  std::ifstream stdout_file(stdout_path);
  std::ifstream stderr_file(stderr_path);
  const std::string stdout_text((std::istreambuf_iterator<char>(stdout_file)),
                                std::istreambuf_iterator<char>());
  const std::string stderr_text((std::istreambuf_iterator<char>(stderr_file)),
                                std::istreambuf_iterator<char>());
  std::remove(stdout_path.c_str());
  std::remove(stderr_path.c_str());

  const int exit_status = WIFEXITED(wait_status) ? WEXITSTATUS(wait_status) : -1;
  return {exit_status, stdout_text, stderr_text};
}
#endif

class RecordingHooks : public DefaultHooks {
 public:
  using DefaultHooks::Open;

  Status Open(const Options& options, const std::string& name,
              std::unique_ptr<DB>* dbptr) override {
    captured_options = std::make_unique<Options>(options);
    return DefaultHooks::Open(options, name, dbptr);
  }

  std::unique_ptr<Options> captured_options;
};
}  // namespace

class DBBenchTest : public testing::Test {
 public:
  DBBenchTest() : rnd_(0xFB) {
    test_path_ = test::PerThreadDBPath("db_bench_test");
    Env::Default()->CreateDir(test_path_);
    db_path_ = test_path_ + "/db";
    wal_path_ = test_path_ + "/wal";
  }

  ~DBBenchTest() {
    //  DestroyDB(db_path_, Options());
  }

  void SetUp() override { ResetArgs(); }

  void ResetArgs() {
    argc_ = 0;
    cursor_ = 0;
    memset(arg_buffer_, 0, kArgBufferSize);
    FLAGS_experimental_compaction_input_path.clear();
    FLAGS_experimental_compaction_output_path.clear();
    FLAGS_experimental_compaction_output_path_id = 0;
  }

  void AppendArgs(const std::vector<std::string>& args) {
    for (const auto& arg : args) {
      ASSERT_LE(cursor_ + arg.size() + 1, kArgBufferSize);
      ASSERT_LE(argc_ + 1, kMaxArgCount);
      snprintf(arg_buffer_ + cursor_, arg.size() + 1, "%s", arg.c_str());

      argv_[argc_++] = arg_buffer_ + cursor_;
      cursor_ += arg.size() + 1;
    }
  }

  // Gets the default options for this test/db_bench.
  // Note that db_bench may change some of the default option values and that
  // the database might as well.  The options changed by db_bench are
  // specified here; the ones by the DB are set via SanitizeOptions
  Options GetDefaultOptions(CompactionStyle style = kCompactionStyleLevel,
                            int levels = 7) const {
    Options opt;

    opt.create_if_missing = true;
    opt.max_open_files = 256;
    opt.max_background_compactions = 10;
    opt.dump_malloc_stats = true;  // db_bench uses a different default
    opt.compaction_style = style;
    opt.num_levels = levels;
    opt.compression = kNoCompression;
    opt.arena_block_size = 8388608;

    return SanitizeOptions(db_path_, opt);
  }

  void RunDbBench(const std::string& options_file_name) {
    GFLAGS_NAMESPACE::FlagSaver flag_saver;
    ResetArgs();
    AppendArgs({"./db_bench", "--benchmarks=fillseq", "--use_existing_db=0",
                "--num=1000", "--compression_type=none",
                std::string(std::string("--db=") + db_path_).c_str(),
                std::string(std::string("--wal_dir=") + wal_path_).c_str(),
                std::string(std::string("--options_file=") + options_file_name)
                    .c_str()});
    ASSERT_EQ(0, db_bench_tool(argc(), argv()));
  }

  // Every flag is passed explicitly because gflags state persists across
  // db_bench_tool() calls within the same test process.
  void RunIngestBench(int batch_size, int num_batches, int file_opening_threads,
                      bool use_file_info, bool fill_cache) {
    GFLAGS_NAMESPACE::FlagSaver flag_saver;
    ResetArgs();
    AppendArgs(
        {"./db_bench", "--benchmarks=ingestexternalfile", "--use_existing_db=0",
         "--num=2000", "--compression_type=none", "--db=" + db_path_,
         "--wal_dir=" + wal_path_,
         "--ingest_external_file_batch_size=" + std::to_string(batch_size),
         "--ingest_external_file_num_batches=" + std::to_string(num_batches),
         "--ingest_external_file_file_opening_threads=" +
             std::to_string(file_opening_threads),
         "--ingest_external_file_use_file_info=" +
             std::string(use_file_info ? "true" : "false"),
         "--ingest_external_file_fill_cache=" +
             std::string(fill_cache ? "true" : "false")});
    ASSERT_EQ(0, db_bench_tool(argc(), argv()));
  }

  void VerifyOptions(const Options& opt) {
    DBOptions loaded_db_opts;
    ConfigOptions config_opts;
    config_opts.ignore_unknown_options = false;
    config_opts.input_strings_escaped = true;
    config_opts.env = Env::Default();
    std::vector<ColumnFamilyDescriptor> cf_descs;
    ASSERT_OK(
        LoadLatestOptions(config_opts, db_path_, &loaded_db_opts, &cf_descs));

    ConfigOptions exact;
    exact.input_strings_escaped = false;
    exact.sanity_level = ConfigOptions::kSanityLevelExactMatch;
    ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(exact, DBOptions(opt),
                                                    loaded_db_opts));
    ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
        exact, ColumnFamilyOptions(opt), cf_descs[0].options));

    // check with the default rocksdb options and expect failure
    ASSERT_NOK(RocksDBOptionsParser::VerifyDBOptions(exact, DBOptions(),
                                                     loaded_db_opts));
    ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
        exact, ColumnFamilyOptions(), cf_descs[0].options));
  }

  static size_t CountFilesAtLevel(const std::vector<LiveFileMetaData>& files,
                                  int level) {
    size_t count = 0;
    for (const auto& file : files) {
      if (file.level == level) {
        ++count;
      }
    }
    return count;
  }

  static size_t CountFilesAtPathAndLevel(
      const std::vector<LiveFileMetaData>& files, const std::string& path,
      int level) {
    size_t count = 0;
    for (const auto& file : files) {
      if (file.level == level && file.db_path == path) {
        ++count;
      }
    }
    return count;
  }

  std::string NewFixturePath(const std::string& name) const {
    static std::atomic<uint64_t> next_fixture_id{0};
    return test_path_ + "/" + name + "_" +
           std::to_string(next_fixture_id.fetch_add(1));
  }

  Options MakeCompactionFixtureOptions(const std::string& fixture_db) const {
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.disable_auto_compactions = true;
    options.num_levels = 3;
    options.db_paths = {{fixture_db, 0}};
    options.cf_paths.clear();
    return options;
  }

  void SeedCompactionFixture(const Options& options, size_t* l0_count,
                             size_t* l1_count, bool overlap_l1 = false) {
    std::unique_ptr<DB> db;
    ASSERT_OK(DB::Open(options, options.db_paths[0].path, &db));
    for (int file = 0; file < 2; ++file) {
      for (int key = 0; key < 100; ++key) {
        ASSERT_OK(db->Put(WriteOptions(),
                          "key-" + std::to_string(file * 100 + key),
                          "value"));
      }
      ASSERT_OK(db->Flush(FlushOptions()));
    }

    std::vector<LiveFileMetaData> files;
    db->GetLiveFilesMetaData(&files);
    std::vector<std::string> input_file_names;
    for (const auto& file : files) {
      if (file.level == 0) {
        input_file_names.push_back(file.name);
      }
    }
    ASSERT_GT(input_file_names.size(), 0U);
    ASSERT_OK(db->CompactFiles(CompactionOptions(), input_file_names, 1, 0));

    for (int file = 2; file < 4; ++file) {
      for (int key = 0; key < 100; ++key) {
        const int key_number = overlap_l1 ? key : file * 100 + key;
        ASSERT_OK(db->Put(WriteOptions(),
                          "key-" + std::to_string(key_number),
                          "value"));
      }
      ASSERT_OK(db->Flush(FlushOptions()));
    }

    files.clear();
    db->GetLiveFilesMetaData(&files);
    *l0_count = CountFilesAtLevel(files, 0);
    *l1_count = CountFilesAtLevel(files, 1);
    ASSERT_GT(*l0_count, 0U);
    ASSERT_GT(*l1_count, 0U);
  }

  void RunCompact0(const std::string& fixture_db,
                   const std::string& options_file_name) {
    GFLAGS_NAMESPACE::FlagSaver flag_saver;
    ResetArgs();
    AppendArgs({"./db_bench", "--benchmarks=compact0",
                "--use_existing_db=true", "--compression_type=none",
                "--num=1", "--threads=1", "--disable_auto_compactions=true",
                "--db=" + fixture_db,
                "--options_file=" + options_file_name});
    ASSERT_EQ(0, db_bench_tool(argc(), argv()));
  }

  void RunCompact0WithPaths(const std::string& fixture_db,
                            const std::string& options_file_name,
                            const std::string& output_path, int output_path_id) {
    GFLAGS_NAMESPACE::FlagSaver flag_saver;
    ResetArgs();
    AppendArgs({"./db_bench", "--benchmarks=compact0",
                "--use_existing_db=true", "--compression_type=none",
                "--num=1", "--threads=1", "--disable_auto_compactions=true",
                "--db=" + fixture_db,
                "--options_file=" + options_file_name,
                "--experimental_compaction_input_path=" + fixture_db,
                "--experimental_compaction_output_path=" + output_path,
                "--experimental_compaction_output_path_id=" +
                    std::to_string(output_path_id)});
    ASSERT_EQ(0, db_bench_tool(argc(), argv()));
  }

  char** argv() { return argv_; }

  int argc() { return argc_; }

  std::string db_path_;
  std::string test_path_;
  std::string wal_path_;

  char arg_buffer_[kArgBufferSize];
  char* argv_[kMaxArgCount];
  int argc_ = 0;
  int cursor_ = 0;
  Random rnd_;
  GFLAGS_NAMESPACE::FlagSaver flag_saver_;
};

namespace {}  // namespace

TEST_F(DBBenchTest, OptionsFile) {
  const std::string kOptionsFileName = test_path_ + "/OPTIONS_test";
  Options opt = GetDefaultOptions();
  ASSERT_OK(PersistRocksDBOptions(WriteOptions(), DBOptions(opt), {"default"},
                                  {ColumnFamilyOptions(opt)}, kOptionsFileName,
                                  opt.env->GetFileSystem().get()));

  // override the following options as db_bench will not take these
  // options from the options file
  opt.wal_dir = wal_path_;

  RunDbBench(kOptionsFileName);
  opt.delayed_write_rate = 16 * 1024 * 1024;  // Set by SanitizeOptions

  VerifyOptions(opt);
}

TEST_F(DBBenchTest, CompactionIOFlagsOverrideOptionsFile) {
  const std::string fixture_db = NewFixturePath("compaction_io_options");
  const std::string options_file_name =
      NewFixturePath("OPTIONS_compaction_io_options");
  Options options = MakeCompactionFixtureOptions(fixture_db);
  options.allow_mmap_reads = true;
  options.allow_mmap_writes = true;
  options.use_direct_reads = false;
  options.use_direct_io_for_compaction_reads = false;
  options.use_direct_io_for_flush_and_compaction = false;
  size_t initial_l0_count = 0;
  size_t initial_l1_count = 0;
  SeedCompactionFixture(options, &initial_l0_count, &initial_l1_count);
  ASSERT_OK(PersistRocksDBOptions(
      WriteOptions(), DBOptions(options), {"default"},
      {ColumnFamilyOptions(options)}, options_file_name,
      options.env->GetFileSystem().get()));

  GFLAGS_NAMESPACE::FlagSaver flag_saver;
  ResetArgs();
  AppendArgs({"./db_bench", "--benchmarks=stats", "--use_existing_db=true",
              "--num=1", "--db=" + fixture_db,
              "--options_file=" + options_file_name,
              "--experimental_compaction_io_depth=2",
              "--max_background_compactions=1", "--subcompactions=1",
              "--mmap_read=false", "--mmap_write=false",
              "--report_bg_io_stats=true",
              "--use_direct_reads=true",
              "--use_direct_io_for_compaction_reads=false",
              "--use_direct_io_for_flush_and_compaction=true"});
  RecordingHooks hooks;
  ASSERT_EQ(0, db_bench_tool(argc(), argv(), hooks));
  ASSERT_NE(nullptr, hooks.captured_options);
  EXPECT_EQ(1, hooks.captured_options->max_background_compactions);
  EXPECT_EQ(1U, hooks.captured_options->max_subcompactions);
  EXPECT_FALSE(hooks.captured_options->allow_mmap_reads);
  EXPECT_FALSE(hooks.captured_options->allow_mmap_writes);
  EXPECT_TRUE(hooks.captured_options->report_bg_io_stats);
  EXPECT_TRUE(hooks.captured_options->use_direct_reads);
  EXPECT_FALSE(hooks.captured_options->use_direct_io_for_compaction_reads);
  EXPECT_TRUE(hooks.captured_options->use_direct_io_for_flush_and_compaction);
}

TEST_F(DBBenchTest, CompactionIOPathsOverrideOptionsFileCfPaths) {
  const std::string fixture_db = NewFixturePath("compaction_io_cf_paths");
  const std::string output_path = NewFixturePath("compaction_io_cf_paths_output");
  const std::string options_file_name =
      NewFixturePath("OPTIONS_compaction_io_cf_paths");
  Options options = MakeCompactionFixtureOptions(fixture_db);
  options.cf_paths = {{NewFixturePath("legacy_cf_path"), 0}};
  ASSERT_OK(Env::Default()->CreateDir(output_path));
  ASSERT_OK(PersistRocksDBOptions(
      WriteOptions(), DBOptions(options), {"default"},
      {ColumnFamilyOptions(options)}, options_file_name,
      options.env->GetFileSystem().get()));

  GFLAGS_NAMESPACE::FlagSaver flag_saver;
  ResetArgs();
  AppendArgs({"./db_bench", "--benchmarks=stats", "--use_existing_db=false",
              "--num=1", "--db=" + fixture_db,
              "--options_file=" + options_file_name,
              "--experimental_compaction_io_depth=2",
              "--experimental_compaction_input_path=" + fixture_db,
              "--experimental_compaction_output_path=" + output_path,
              "--experimental_compaction_output_path_id=1"});
  RecordingHooks hooks;
  ASSERT_EQ(0, db_bench_tool(argc(), argv(), hooks));
  ASSERT_NE(nullptr, hooks.captured_options);
  EXPECT_TRUE(hooks.captured_options->cf_paths.empty());
  ASSERT_EQ(2U, hooks.captured_options->db_paths.size());
  EXPECT_EQ(fixture_db, hooks.captured_options->db_paths[0].path);
  EXPECT_EQ(output_path, hooks.captured_options->db_paths[1].path);
}

TEST_F(DBBenchTest, Compact0DefaultOutputPath) {
  const std::string fixture_db = NewFixturePath("compact0_default");
  const std::string options_file_name =
      NewFixturePath("OPTIONS_compact0_default");
  Options options = MakeCompactionFixtureOptions(fixture_db);
  size_t initial_l0_count = 0;
  size_t initial_l1_count = 0;
  SeedCompactionFixture(options, &initial_l0_count, &initial_l1_count);
  ASSERT_OK(PersistRocksDBOptions(
      WriteOptions(), DBOptions(options), {"default"},
      {ColumnFamilyOptions(options)}, options_file_name,
      options.env->GetFileSystem().get()));

  RunCompact0(fixture_db, options_file_name);

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, fixture_db, &db));
  std::vector<LiveFileMetaData> files;
  db->GetLiveFilesMetaData(&files);
  EXPECT_EQ(0U, CountFilesAtLevel(files, 0));
  EXPECT_GE(CountFilesAtLevel(files, 1), initial_l1_count);
  for (const auto& file : files) {
    if (file.level >= 1) {
      EXPECT_EQ(fixture_db, file.db_path);
    }
  }
  EXPECT_EQ(initial_l0_count, 2U);
}

TEST_F(DBBenchTest, Compact0RoutesOutputToPathOne) {
  const std::string fixture_db = NewFixturePath("compact0_path_one");
  const std::string output_path = NewFixturePath("compact0_path_one_output");
  const std::string options_file_name =
      NewFixturePath("OPTIONS_compact0_path_one");
  const std::string options_file_path =
      NewFixturePath("compact0_options_path");
  Options seed_options = MakeCompactionFixtureOptions(fixture_db);
  size_t initial_l0_count = 0;
  size_t initial_l1_count = 0;
  SeedCompactionFixture(seed_options, &initial_l0_count, &initial_l1_count,
                        true);
  seed_options.db_paths = {{options_file_path, 0}};
  ASSERT_OK(PersistRocksDBOptions(
      WriteOptions(), DBOptions(seed_options), {"default"},
      {ColumnFamilyOptions(seed_options)}, options_file_name,
      seed_options.env->GetFileSystem().get()));

  RunCompact0WithPaths(fixture_db, options_file_name, output_path, 1);

  Options routed_options = MakeCompactionFixtureOptions(fixture_db);
  routed_options.db_paths = {{fixture_db, 0}, {output_path, 0}};
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(routed_options, fixture_db, &db));
  std::vector<LiveFileMetaData> files;
  db->GetLiveFilesMetaData(&files);
  EXPECT_EQ(0U, CountFilesAtLevel(files, 0));
  EXPECT_EQ(0U, CountFilesAtPathAndLevel(files, fixture_db, 1));
  size_t output_file_count = 0;
  for (const auto& file : files) {
    if (file.level != 1 || file.directory != output_path) {
      continue;
    }
    ++output_file_count;
    EXPECT_EQ(file.db_path, file.directory);
    EXPECT_EQ(file.name, "/" + file.relative_filename);
    uint64_t file_size = 0;
    ASSERT_OK(Env::Default()->GetFileSize(
        output_path + "/" + file.relative_filename, &file_size));
    EXPECT_EQ(file.size, file_size);
  }
  EXPECT_GT(output_file_count, 0U);
  EXPECT_EQ(initial_l0_count, 2U);
  EXPECT_GT(initial_l1_count, 0U);
}

#ifndef OS_WIN
TEST_F(DBBenchTest, NormalInvocationDoesNotPrintCompactionPathRouting) {
  const SubprocessResult result = RunDbBenchSubprocess(
      {"./db_bench", "--benchmarks=stats", "--use_existing_db=false",
       "--compression_type=none", "--num=1", "--seed=1",
       "--db=" + NewFixturePath("normal_no_compaction_routing")},
      test_path_);
  EXPECT_EQ(0, result.exit_status);
  EXPECT_EQ(result.stdout_text.find("Compaction path routing:"),
            std::string::npos);
}

TEST_F(DBBenchTest, EmitsAutomaticCompactionJobRecords) {
  const SubprocessResult result = RunDbBenchSubprocess(
      {"./db_bench", "--benchmarks=fillrandom", "--use_existing_db=false",
       "--compression_type=none", "--num=20000", "--value_size=1000",
       "--write_buffer_size=4096", "--max_write_buffer_number=2",
       "--level0_file_num_compaction_trigger=2",
       "--target_file_size_base=4096", "--max_bytes_for_level_base=8192",
       "--max_background_compactions=1", "--subcompactions=1",
       "--experimental_compaction_io_depth=2",
       "--db=" + NewFixturePath("automatic_compaction_records")},
      test_path_);
  ASSERT_EQ(0, result.exit_status);
  EXPECT_NE(result.stdout_text.find("\"compaction_io_experiment\""),
            std::string::npos);
  EXPECT_NE(result.stdout_text.find("\"job_id\""), std::string::npos);
  EXPECT_NE(result.stdout_text.find("\"input_level\""), std::string::npos);
  EXPECT_NE(result.stdout_text.find("\"output_level\""), std::string::npos);
  EXPECT_NE(result.stdout_text.find("\"output_sync_us\""),
            std::string::npos);
}

TEST_F(DBBenchTest, Compact0RejectsOutOfRangeOutputPathId) {
  const std::string fixture_db = NewFixturePath("compact0_out_of_range");
  const std::string output_path = NewFixturePath("compact0_out_of_range_output");
  const std::string options_file_name =
      NewFixturePath("OPTIONS_compact0_out_of_range");
  Options options = MakeCompactionFixtureOptions(fixture_db);
  size_t initial_l0_count = 0;
  size_t initial_l1_count = 0;
  SeedCompactionFixture(options, &initial_l0_count, &initial_l1_count);
  ASSERT_OK(Env::Default()->CreateDir(output_path));
  ASSERT_OK(PersistRocksDBOptions(
      WriteOptions(), DBOptions(options), {"default"},
      {ColumnFamilyOptions(options)}, options_file_name,
      options.env->GetFileSystem().get()));

  const SubprocessResult result = RunDbBenchSubprocess(
      {"./db_bench", "--benchmarks=compact0", "--use_existing_db=true",
       "--compression_type=none", "--num=1", "--threads=1",
       "--disable_auto_compactions=true", "--db=" + fixture_db,
       "--options_file=" + options_file_name,
       "--experimental_compaction_input_path=" + fixture_db,
       "--experimental_compaction_output_path=" + output_path,
       "--experimental_compaction_output_path_id=2"},
      test_path_);
  EXPECT_EQ(1, result.exit_status);
  EXPECT_NE(result.stderr_text.find("Invalid compaction output path ID"),
            std::string::npos);
}

TEST_F(DBBenchTest, Compact0RejectsNegativeOutputPathId) {
  const SubprocessResult result = RunDbBenchSubprocess(
      {"./db_bench", "--benchmarks=compact0", "--use_existing_db=true",
       "--db=" + NewFixturePath("compact0_negative_path_id"),
       "--experimental_compaction_input_path=" +
           NewFixturePath("compact0_negative_input"),
       "--experimental_compaction_output_path=" +
           NewFixturePath("compact0_negative_output"),
       "--experimental_compaction_output_path_id=-1"},
      test_path_);
  EXPECT_EQ(1, result.exit_status);
  EXPECT_NE(result.stderr_text.find("Invalid compaction output path ID"),
            std::string::npos);
}

TEST_F(DBBenchTest, Compact0RejectsMalformedOutputPathId) {
  const SubprocessResult result = RunDbBenchSubprocess(
      {"./db_bench", "--benchmarks=compact0", "--use_existing_db=true",
       "--db=" + NewFixturePath("compact0_malformed_path_id"),
       "--experimental_compaction_output_path_id=not-an-integer"},
      test_path_);
  EXPECT_EQ(1, result.exit_status);
  EXPECT_TRUE(result.stderr_text.find("illegal value") != std::string::npos ||
              result.stderr_text.find("Invalid value") != std::string::npos ||
              result.stderr_text.find("invalid value") != std::string::npos);
}

TEST_F(DBBenchTest, Compact0RejectsMissingPathPair) {
  const SubprocessResult result = RunDbBenchSubprocess(
      {"./db_bench", "--benchmarks=compact0", "--use_existing_db=true",
       "--db=" + NewFixturePath("compact0_missing_path_pair"),
       "--experimental_compaction_input_path=" +
           NewFixturePath("compact0_missing_output")},
      test_path_);
  EXPECT_EQ(1, result.exit_status);
  EXPECT_NE(result.stderr_text.find(
                "Both experimental compaction input and output paths are required"),
            std::string::npos);
}

TEST_F(DBBenchTest, Compact0RejectsMissingOptionsFile) {
  const std::string options_file_name = NewFixturePath("OPTIONS_missing");
  const SubprocessResult result = RunDbBenchSubprocess(
      {"./db_bench", "--benchmarks=compact0", "--use_existing_db=true",
       "--db=" + NewFixturePath("compact0_missing_options"),
       "--options_file=" + options_file_name},
      test_path_);
  EXPECT_EQ(1, result.exit_status);
  EXPECT_NE(result.stderr_text.find("Unable to load options file"),
            std::string::npos);
}

TEST_F(DBBenchTest, Compact0RejectsMalformedOptionsFile) {
  const std::string options_file_name = test_path_ + "/OPTIONS_malformed";
  std::unique_ptr<WritableFile> writable;
  ASSERT_OK(Env::Default()->NewWritableFile(options_file_name, &writable,
                                             EnvOptions()));
  ASSERT_OK(writable->Append("not a RocksDB options file\n"));
  ASSERT_OK(writable->Close());

  const SubprocessResult result = RunDbBenchSubprocess(
      {"./db_bench", "--benchmarks=compact0", "--use_existing_db=true",
       "--db=" + NewFixturePath("compact0_malformed_options"),
       "--options_file=" + options_file_name},
      test_path_);
  EXPECT_EQ(1, result.exit_status);
  EXPECT_NE(result.stderr_text.find("Unable to load options file"),
            std::string::npos);
}
#endif

TEST_F(DBBenchTest, IngestExternalFile) {
  // Exercise the ingestexternalfile benchmark with both serial and parallel
  // (file_opening_threads > 1) commit-time table-reader opening.
  for (int file_opening_threads : {1, 4}) {
    RunIngestBench(/*batch_size=*/3, /*num_batches=*/2, file_opening_threads,
                   /*use_file_info=*/false, /*fill_cache=*/true);
  }
}

TEST_F(DBBenchTest, IngestExternalFileWithFileInfo) {
  RunIngestBench(/*batch_size=*/3, /*num_batches=*/2,
                 /*file_opening_threads=*/4,
                 /*use_file_info=*/true, /*fill_cache=*/true);
}

TEST_F(DBBenchTest, IngestExternalFileWithoutFillCache) {
  RunIngestBench(/*batch_size=*/3, /*num_batches=*/2,
                 /*file_opening_threads=*/1,
                 /*use_file_info=*/false, /*fill_cache=*/false);
}

TEST_F(DBBenchTest, OptionsFileUniversal) {
  const std::string kOptionsFileName = test_path_ + "/OPTIONS_test";

  Options opt = GetDefaultOptions(kCompactionStyleUniversal, 1);

  ASSERT_OK(PersistRocksDBOptions(WriteOptions(), DBOptions(opt), {"default"},
                                  {ColumnFamilyOptions(opt)}, kOptionsFileName,
                                  opt.env->GetFileSystem().get()));

  // override the following options as db_bench will not take these
  // options from the options file
  opt.wal_dir = wal_path_;
  RunDbBench(kOptionsFileName);

  VerifyOptions(opt);
}

TEST_F(DBBenchTest, OptionsFileMultiLevelUniversal) {
  const std::string kOptionsFileName = test_path_ + "/OPTIONS_test";

  Options opt = GetDefaultOptions(kCompactionStyleUniversal, 12);

  ASSERT_OK(PersistRocksDBOptions(WriteOptions(), DBOptions(opt), {"default"},
                                  {ColumnFamilyOptions(opt)}, kOptionsFileName,
                                  opt.env->GetFileSystem().get()));

  // override the following options as db_bench will not take these
  // options from the options file
  opt.wal_dir = wal_path_;

  RunDbBench(kOptionsFileName);
  VerifyOptions(opt);
}

const std::string options_file_content = R"OPTIONS_FILE(
[Version]
  rocksdb_version=4.3.1
  options_file_version=1.1

[DBOptions]
  wal_bytes_per_sync=1048576
  delete_obsolete_files_period_micros=0
  WAL_ttl_seconds=0
  WAL_size_limit_MB=0
  db_write_buffer_size=0
  max_subcompactions=1
  table_cache_numshardbits=4
  max_open_files=-1
  max_file_opening_threads=10
  max_background_compactions=5
  use_fsync=false
  use_adaptive_mutex=false
  max_total_wal_size=18446744073709551615
  compaction_readahead_size=0
  keep_log_file_num=10
  skip_stats_update_on_db_open=false
  max_manifest_file_size=18446744073709551615
  db_log_dir=
  writable_file_max_buffer_size=1048576
  paranoid_checks=true
  is_fd_close_on_exec=true
  bytes_per_sync=1048576
  enable_thread_tracking=true
  recycle_log_file_num=0
  create_missing_column_families=false
  log_file_time_to_roll=0
  max_background_flushes=1
  create_if_missing=true
  error_if_exists=false
  delayed_write_rate=1048576
  manifest_preallocation_size=4194304
  allow_mmap_reads=false
  allow_mmap_writes=false
  use_direct_reads=false
  use_direct_io_for_flush_and_compaction=false
  stats_dump_period_sec=600
  allow_fallocate=true
  max_log_file_size=83886080
  advise_random_on_open=true
  dump_malloc_stats=true

[CFOptions "default"]
  compaction_filter_factory=nullptr
  table_factory=BlockBasedTable
  prefix_extractor=nullptr
  comparator=leveldb.BytewiseComparator
  compression_per_level=
  max_bytes_for_level_base=104857600
  bloom_locality=0
  target_file_size_base=10485760
  memtable_huge_page_size=0
  max_successive_merges=1000
  max_sequential_skip_in_iterations=8
  arena_block_size=52428800
  target_file_size_multiplier=1
  source_compaction_factor=1
  min_write_buffer_number_to_merge=1
  max_write_buffer_number=2
  write_buffer_size=419430400
  max_grandparent_overlap_factor=10
  max_bytes_for_level_multiplier=10
  memtable_factory=SkipListFactory
  compression=kNoCompression
  min_partial_merge_operands=2
  level0_stop_writes_trigger=100
  num_levels=1
  level0_slowdown_writes_trigger=50
  level0_file_num_compaction_trigger=10
  expanded_compaction_factor=25
  max_write_buffer_size_to_maintain=0
  verify_checksums_in_compaction=true
  merge_operator=nullptr
  memtable_prefix_bloom_bits=0
  memtable_whole_key_filtering=true
  paranoid_file_checks=false
  inplace_update_num_locks=10000
  optimize_filters_for_hits=false
  level_compaction_dynamic_level_bytes=false
  inplace_update_support=false
  compaction_style=kCompactionStyleUniversal
  memtable_prefix_bloom_probes=6
  filter_deletes=false
  hard_pending_compaction_bytes_limit=0
  disable_auto_compactions=false
  compaction_measure_io_stats=false
  enable_blob_files=true
  min_blob_size=16
  blob_file_size=10485760
  blob_compression_type=kNoCompression
  enable_blob_garbage_collection=true
  blob_garbage_collection_age_cutoff=0.5
  blob_garbage_collection_force_threshold=0.75
  blob_compaction_readahead_size=262144
  blob_file_starting_level=0
  prepopulate_blob_cache=kDisable;

[TableOptions/BlockBasedTable "default"]
  format_version=0
  skip_table_builder_flush=false
  cache_index_and_filter_blocks=false
  flush_block_policy_factory=FlushBlockBySizePolicyFactory
  index_type=kBinarySearch
  whole_key_filtering=true
  checksum=kCRC32c
  no_block_cache=false
  block_size=32768
  block_size_deviation=10
  block_restart_interval=16
  filter_policy=rocksdb.BuiltinBloomFilter
)OPTIONS_FILE";

TEST_F(DBBenchTest, OptionsFileFromFile) {
  const std::string kOptionsFileName = test_path_ + "/OPTIONS_flash";
  std::unique_ptr<WritableFile> writable;
  ASSERT_OK(Env::Default()->NewWritableFile(kOptionsFileName, &writable,
                                            EnvOptions()));
  ASSERT_OK(writable->Append(options_file_content));
  ASSERT_OK(writable->Close());

  DBOptions db_opt;
  ConfigOptions config_opt;
  config_opt.ignore_unknown_options = false;
  config_opt.input_strings_escaped = true;
  config_opt.env = Env::Default();
  std::vector<ColumnFamilyDescriptor> cf_descs;
  ASSERT_OK(
      LoadOptionsFromFile(config_opt, kOptionsFileName, &db_opt, &cf_descs));
  Options opt(db_opt, cf_descs[0].options);
  opt.create_if_missing = true;

  // override the following options as db_bench will not take these
  // options from the options file
  opt.wal_dir = wal_path_;

  RunDbBench(kOptionsFileName);

  VerifyOptions(SanitizeOptions(db_path_, opt));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ROCKSDB_NAMESPACE::db_bench_path =
      ROCKSDB_NAMESPACE::GetDbBenchPath(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

#else

int main(int argc, char** argv) {
  std::printf("Skip db_bench_tool_test as the required library GFLAG is missing.");
}
#endif  // #ifdef GFLAGS
