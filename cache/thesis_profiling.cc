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


void ThesisProfiling::start(std::string result_file_path) {
  if (result_file_path != "") {
    result_file.open(result_file_path);
    assert(result_file.is_open());
    result_file << "METRIC";
    for (auto const& tt : thread_t_names) {
      result_file << "," << tt;
    }
    result_file << std::endl;
    writer = std::thread(&ThesisProfiling::cycle, this);
  }   
}

void ThesisProfiling::stop() {
  if (result_file.is_open()) {
    stopWriter = true;
    writer.join();
  }
}

void ThesisProfiling::insert(size_t keysize, size_t valuesize) {
  auto const thread_type = getThread_t();
  std::lock_guard<std::mutex> guard(profiles_mutex);
  counter[thread_type][INSERT]++;
  counter[thread_type][INS_MBYTES] += ((int)(keysize + valuesize));
}

void ThesisProfiling::lookup(bool hit) {
  auto const thread_type = getThread_t();
  std::lock_guard<std::mutex> guard(profiles_mutex);
  counter[thread_type][LOOKUP]++;
  if (hit) {
    counter[thread_type][HITS]++;
  }
}

void ThesisProfiling::dump() {
  std::lock_guard<std::mutex> guard(profiles_mutex);
  for (int i = 0; i < NUM_PROFILE_T - 1; i++) {
    result_file << profile_t_names[i];
    for (int j = 0; j < NUM_THREAD_T; j++) {
      result_file << "," << counter[j][i];
    }
    result_file << std::endl;
  }
  result_file << "hitratio";
  for (int i = 0; i < NUM_THREAD_T; i++) {
    result_file << "," << (float)counter[i][HITS] / (float)counter[i][LOOKUP];
  }
  result_file << std::endl;
}

void ThesisProfiling::reset() {
  std::lock_guard<std::mutex> guard(profiles_mutex);
  memset(counter, 0, sizeof(counter));
}



void ThesisProfiling::cycle() {
  while (!stopWriter) {
    dump();
    reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
  dump();
}