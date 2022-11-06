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
#include <atomic>
#include "rocksdb/env.h"

enum thread_t {UNKNOWN = 0, COMPACTION, FLUSH, FOREGROUND, NUM_THREAD_T};

static auto thread_tToString(const thread_t x) -> std::string {
  switch (x) {
    case UNKNOWN:
      return "unknown";
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
  int inserts {0};
  int look_ups {0};
  int hits {0};
};

class ThesisProfiling {
 public:
  ThesisProfiling() {
    trace_file.open("trace_file.json");
    assert(trace_file.is_open());
    writer = std::thread (&ThesisProfiling::cycle, this);
  }

  ~ThesisProfiling() {
    stopWriter = true;
    writer.join();
    trace_file.close();
  }

  void writeCounter() {
    std::lock_guard<std::mutex> guard (profiles_mutex);
    trace_file << "    {\n";
    for (int i = 0; i < NUM_THREAD_T - 1; i++) {
      auto const& entry = counter[i];
      trace_file << "      \"" << thread_tToString((thread_t) i) << "\" : {\n" 
                 << "        \"I\" : " << entry.inserts << ",\n"
                 << "        \"L\" : " << entry.look_ups << "\n"
                 << "      },\n";
    }
    auto const& entry = counter[NUM_THREAD_T - 1];
    trace_file << "      \"" << thread_tToString((thread_t) (NUM_THREAD_T - 1)) << "\" : {\n" 
               << "        \"I\" : " << entry.inserts << ",\n"
               << "        \"L\" : " << entry.look_ups << "\n"
               << "      }\n";
    if (stopWriter) {
      trace_file << "    }\n";
    } else {
      trace_file << "    },\n";
    }
  }

  void writeCounterAccum() {
    std::lock_guard<std::mutex> guard (profiles_mutex);
    for (int i = 0; i < NUM_THREAD_T - 1; i++) {
      auto const& entry = counter[i];
      trace_file << "    \"" << thread_tToString((thread_t) i) << "\" : {\n" 
                 << "      \"I\" : " << entry.inserts << ",\n"
                 << "      \"L\" : " << entry.look_ups << ",\n"
                 << "      \"HR\" : " << (entry.look_ups ? (float) entry.hits / (float) entry.look_ups : 0) << "\n"
                 << "    },\n";
    }
    auto const& entry = counter[NUM_THREAD_T - 1];
    trace_file << "    \"" << thread_tToString((thread_t) (NUM_THREAD_T - 1)) << "\" : {\n" 
               << "      \"I\" : " << entry.inserts << ",\n"
               << "      \"L\" : " << entry.look_ups << ",\n"
               << "      \"HR\" : " << (entry.look_ups ? (float) entry.hits / (float) entry.look_ups : 0) << "\n"
               << "    }\n";
    trace_file << "  }\n";
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
    trace_file << "{\n  \"time-series\" : [\n";
    while (!stopWriter) {
      writeCounter();
      resetCounter();
      std::this_thread::sleep_for(
     std::chrono::milliseconds(1000));
    }
    writeCounter();
    trace_file << "  ],\n  \"total\" : {\n";
    writeCounterAccum();
    trace_file << "}\n";
  }

 private:

  Profile counter[NUM_THREAD_T]; 
  Profile counter_accum[NUM_THREAD_T];
  std::mutex profiles_mutex;

  std::ofstream trace_file;

  std::atomic_bool stopWriter {false};
  std::thread writer;
};
