#pragma once

#include "rocksdb/thread_status.h"
#include <cassert>
#include <fstream>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <chrono>
#include <iostream>
#include "rocksdb/env.h"

enum thread_t {UNKNOWN = 0, COMPACTION, FLUSH, FOREGROUND, NUM_THREAD_T};

static auto thread_tToString(const thread_t x) -> std::string {
  switch (x) {
    case UNKNOWN:
      return "U";
    case COMPACTION:
      return "C";
    case FLUSH:
      return "Fl";
    case FOREGROUND:
      return "Fo";
    default:
      return "";
  };
}

struct Profile {
  int inserts {0};
  int look_ups {0};
  int hits {0};
};

class ThesisProfiling {
 public:
  ThesisProfiling() {
    trace_file.open("trace_file.txt");
    assert(trace_file.is_open());
    writer = std::thread (&ThesisProfiling::cycle, this);
  }

  ~ThesisProfiling() {
    stopWriter = true;
    writer.join();
    trace_file << "-------" << std::endl;
    writeCounterAccum();
    trace_file.close();
  }

  void writeCounter() {
    std::lock_guard<std::mutex> guard (profiles_mutex);
    trace_file << time(0) << "=" << "{ ";
    for (int i = 0; i < NUM_THREAD_T; i++) {
      auto const& entry = counter[i];
      trace_file << thread_tToString((thread_t) i) << ":" << "{ I=" << entry.inserts
                 << ", L=" << entry.look_ups << " }, ";
    }
    trace_file << "}\n";
  }

  void writeCounterAccum() {
    std::lock_guard<std::mutex> guard (profiles_mutex);
    trace_file << "{ ";
    for (int i = 0; i < NUM_THREAD_T; i++) {
      auto const& entry = counter_accum[i];
      trace_file << thread_tToString((thread_t) i) << ":" << "{ I=" << entry.inserts
                 << ", L=" << entry.look_ups << ", HR="
                 << (entry.look_ups
                         ? (float) entry.hits / (float) entry.look_ups
                         : 0) << " }, ";
    }
    trace_file << "}\n";
  }

  void resetCounter() {
    std::lock_guard<std::mutex> guard (profiles_mutex);
    for (auto & entry : counter) {
      entry.inserts = 0;
      entry.hits = 0;
      entry.look_ups = 0;
    }
  }

  thread_t getThread_t() {
    return ({
      std::lock_guard<std::mutex> guard(threads_ops_mutex);
      auto thread_op = threads_ops.find(std::this_thread::get_id());
      thread_op == threads_ops.end()
                             ? FOREGROUND
                             : (thread_t) thread_op->second;
    });
  }

  void insert() {
    auto thread_type = getThread_t();
    std::lock_guard<std::mutex> guard (profiles_mutex);
    counter[thread_type].inserts++;
    counter_accum[thread_type].inserts++;
  }

  void look_up(bool hit) {
    auto thread_type = getThread_t();
    std::lock_guard<std::mutex> guard (profiles_mutex);
    counter[thread_type].look_ups++;
    counter_accum[thread_type].look_ups++;
    if (hit) {
      counter[thread_type].hits++;
      counter_accum[thread_type].hits++;
    }
  }

  void cycle() {
    while (!stopWriter) {
      writeCounter();
      resetCounter();
      std::this_thread::sleep_for(
     std::chrono::milliseconds(1000));
    }
  }

 private:

  Profile counter[NUM_THREAD_T]; 
  Profile counter_accum[NUM_THREAD_T];
  std::mutex profiles_mutex;

  std::ofstream trace_file;

  std::atomic_bool stopWriter {false};
  std::thread writer;
};
