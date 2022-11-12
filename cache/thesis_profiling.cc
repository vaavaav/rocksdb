#include "thesis_profiling.h"
#include <cassert>
#include <chrono>
#include <iostream>
#include <string.h>

std::mutex threads_ops_mutex;
std::map<std::thread::id, ROCKSDB_NAMESPACE::ThreadStatus::OperationType> threads_ops;

thread_t getThread_t() {
  return ({
    std::lock_guard<std::mutex> guard(threads_ops_mutex);
    auto const thread_op = threads_ops.find(std::this_thread::get_id());
    thread_op == threads_ops.end() ? FOREGROUND : (thread_t)thread_op->second;
  });
}


void ThesisProfiling::start(bool _reject_compactions, std::string profiling_results_dir) {
    result_file.open(profiling_results_dir + "/results.csv");
    assert(result_file.is_open());
    result_file << "METRIC";
    for (auto const& tt : thread_t_names) {
      result_file << "," << tt;
    }
    result_file << ",global";
    result_file << std::endl;
    reject_compactions = _reject_compactions;
    writer = std::thread(&ThesisProfiling::cycle, this);
}

void ThesisProfiling::stop() {
  stopWriter = true;
  writer.join();
}

void ThesisProfiling::insert(size_t keysize, size_t valuesize) {
  auto const thread_type = getThread_t();
  std::lock_guard<std::mutex> guard(profiles_mutex);
  counter[thread_type][INSERT]++;
  counter[thread_type][INS_BYTES] += ((int)(keysize + valuesize));
}

bool ThesisProfiling::isReject() {
  auto const thread_type = getThread_t();
  return reject_compactions && thread_type == COMPACTION;
}

void ThesisProfiling::lookup(bool hit) {
  auto const thread_type = getThread_t();
  std::lock_guard<std::mutex> guard(profiles_mutex);
  counter[thread_type][LOOKUP]++;
  if (hit) {
    counter[thread_type][HITS]++;
  }
}

void ThesisProfiling::qps(int qps) {
  global_qps = qps;
}

void ThesisProfiling::dump() {
  result_file << "queries_per_second,,,,," << global_qps << std::endl;
  std::lock_guard<std::mutex> guard(profiles_mutex);
  for (int i = 0; i < NUM_PROFILE_T; i++) {
    result_file << profile_t_names[i] << ",";
    for (int j = 0; j < NUM_THREAD_T; j++) {
      result_file << counter[j][i] << ",";
    }
    result_file << std::endl;
  }
  result_file << "hitratio,";
  auto global_lu {0}; 
  auto global_h  {0};
  for (int i = 0; i < NUM_THREAD_T; i++) {
    global_lu += counter[i][LOOKUP]; 
    global_h += counter[i][HITS]; 
    result_file << (float)counter[i][HITS] / (float)counter[i][LOOKUP] << ",";
  }
  result_file << (float)global_h / (float) global_lu; 
  result_file << std::endl;
}

inline void ThesisProfiling::reset() {
  global_qps = 0;
  std::lock_guard<std::mutex> guard(profiles_mutex);
  memset(counter, 0, sizeof(counter));
}

void ThesisProfiling::cycle() {
  std::this_thread::sleep_for(std::chrono::seconds(1));
  while (!stopWriter) {
    dump();
    reset();
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}