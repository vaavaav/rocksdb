#pragma once

#include "rocksdb/env.h"
#include "rocksdb/thread_status.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string.h>
#include <string>
#include <thread>

enum thread_t {UNKNOWN = 0, COMPACTION, FLUSH, FOREGROUND, NUM_THREAD_T};
static std::string const thread_t_names[NUM_THREAD_T] = {"unknown","compaction","flush","foreground"};
enum profile_t {INSERT = 0, LOOKUP, INS_MBYTES, HITS, NUM_PROFILE_T};
static std::string const profile_t_names[NUM_PROFILE_T] = {"insert","lookup","inserted_mbytes","hits"};

class ThesisProfiling {
 public:
  ThesisProfiling() {
    trace_file.open("trace_file.csv");
    assert(trace_file.is_open());
    trace_file << "METRIC";
    for (auto const& tt : thread_t_names) {
      trace_file << "," << tt;
    }
    trace_file << std::endl;
    writer = std::thread (&ThesisProfiling::cycle, this);
  }

  ~ThesisProfiling() {
    stopWriter = true;
    writer.join();
    trace_file.close();
  }

  void writeCounter() {
    std::lock_guard<std::mutex> guard(profiles_mutex);
    for (int i = 0; i < NUM_PROFILE_T - 1; i++) {
      trace_file << profile_t_names[i];
      for (int j = 0; j < NUM_THREAD_T; j++) {
        trace_file << "," << counter[j][i];
      }
      trace_file << std::endl;
    }
    trace_file << "hitratio";
    for (int i = 0; i < NUM_THREAD_T; i++) {
      auto const& lu = counter[i][LOOKUP];
      trace_file << "," << (lu ? (float) counter[i][HITS] / (float) lu : 0);
    }
    trace_file << std::endl;
  }


  void resetCounter() {
    std::lock_guard<std::mutex> guard (profiles_mutex);
    memset(counter, 0, sizeof(counter));
  }

  thread_t getThread_t() {
    return ({
      std::lock_guard<std::mutex> guard(threads_ops_mutex);
      auto const thread_op = threads_ops.find(std::this_thread::get_id());
      thread_op == threads_ops.end()
                             ? FOREGROUND
                             : (thread_t) thread_op->second;
    });
  }

  void insert(size_t keysize, size_t valuesize) {
    auto const thread_type = getThread_t();
    std::lock_guard<std::mutex> guard (profiles_mutex);
    counter[thread_type][INSERT]++;
    counter[thread_type][INS_MBYTES] += ((int) (keysize + valuesize)) >> 20;
  }

  void lookup(bool hit) {
    auto const thread_type = getThread_t();
    std::lock_guard<std::mutex> guard (profiles_mutex);
    counter[thread_type][LOOKUP]++;
    if (hit) {
      counter[thread_type][HITS]++;
    }
  }

  void cycle() {
    while (!stopWriter) {
      writeCounter();
      resetCounter();
      std::this_thread::sleep_for(
     std::chrono::milliseconds(1000));
    }
    writeCounter();
  }

 private:
  std::mutex profiles_mutex;
  int counter[NUM_THREAD_T][NUM_PROFILE_T] {0}; 

  std::ofstream trace_file;

  std::atomic_bool stopWriter {false};
  std::thread writer;
};
