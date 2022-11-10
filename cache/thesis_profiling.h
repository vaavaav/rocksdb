#pragma once

#include <string>
#include "rocksdb/thread_status.h"
#include <mutex>
#include <thread>
#include <atomic>
#include <fstream>

enum thread_t {UNKNOWN = 0, COMPACTION, FLUSH, FOREGROUND, NUM_THREAD_T};
static std::string const thread_t_names[NUM_THREAD_T] = {"unknown","compaction","flush","foreground"};
enum profile_t {INSERT = 0, LOOKUP, INS_BYTES, HITS, NUM_PROFILE_T};
static std::string const profile_t_names[NUM_PROFILE_T] = {"insert","lookup","inserted_bytes","hits"};


extern std::mutex threads_ops_mutex;
extern std::map<std::thread::id, ROCKSDB_NAMESPACE::ThreadStatus::OperationType> threads_ops;
extern thread_t getThread_t();


class ThesisProfiling {
    public: 
        static ThesisProfiling& getInstance() {
            static ThesisProfiling instance;
            return instance;
        }

        void start(std::string);
        void stop();
        void insert(size_t, size_t);
        void lookup(bool);

    private:
        ThesisProfiling() {};
        ThesisProfiling(ThesisProfiling const&) = delete;
        void operator=(ThesisProfiling const&) = delete;

        void cycle();
        void dump();
        void reset();

        std::mutex profiles_mutex;
        int counter[NUM_THREAD_T][NUM_PROFILE_T]{0};
        std::ofstream result_file;

        std::atomic_bool stopWriter{false};
        std::thread writer;
};