#pragma once

#include "rocksdb/thread_status.h"
#include <atomic>
#include <cassert>
#include <fstream>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <chrono>
#include "rocksdb/env.h"

enum thread_t {UNKNOWN = 0, COMPACTION, FLUSH, FOREGROUND, NUM_THREAD_T};

static auto thread_tToString(const thread_t x) -> std::string {
  switch (x) {
    case UNKNOWN:
      return "other";
    case COMPACTION:
      return "compaction";
    case FLUSH:
      return "flush";
    case FOREGROUND:
      return "foreground";
    default:
      return "";
  };
}

struct Profile {
  std::atomic_int inserts {0};
  std::atomic_int look_ups {0};
  std::atomic_int hits {0};
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
    writeCounter(false, counter_accum);
    trace_file.close();
  }

  void writeCounter(bool printTimestamp, Profile const (& c)[NUM_THREAD_T]) {
    auto now = time(0);
    for (int i = 0; i < NUM_THREAD_T; i++) {
      auto const& entry = c[i];
      if (printTimestamp) {
        trace_file << now << "-"; 
      }
      trace_file << thread_tToString((thread_t) i) << "-" << "I=" << entry.inserts
                 << ",L=" << entry.look_ups << ",HR="
                 << (entry.look_ups
                         ? entry.hits / entry.look_ups
                         : 0)
                 << "\n";
    }
  }

  void resetCounter() {
    for (auto & entry : counter) {
      entry.inserts = 0;
      entry.hits = 0;
      entry.look_ups = 0;
    }
  }

  void insert() {
    auto thread_type = ({
      std::lock_guard<std::mutex> guard(threads_ops_mutex);
      auto thread_op = threads_ops.find(std::this_thread::get_id());
      thread_op == threads_ops.end()
                             ? FOREGROUND
                             : (thread_t)thread_op->second;
    });
    counter[thread_type].inserts++;
    counter_accum[thread_type].inserts++;
  }

  void cycle() {
    while (!stopWriter) {
      writeCounter(true, counter);
      std::this_thread::sleep_for(
     std::chrono::milliseconds(1000));
    }
  }

 private:

  Profile counter[NUM_THREAD_T]; 
  Profile counter_accum[NUM_THREAD_T];

  std::ofstream trace_file;

  std::atomic_bool stopWriter {false};
  std::thread writer;
};
