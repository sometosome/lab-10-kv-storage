// Copyright 2021 Your Name <your_email>

#ifndef INCLUDE_KV_STORAGE_HPP_
#define INCLUDE_KV_STORAGE_HPP_

#include <iostream>
#include <string>
#include <random>
#include <utility>
#include <thread>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <boost/unordered_map.hpp>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <boost/program_options.hpp>
#include <mutex>
#include <vector>
#include <list>
#include <picosha2.hpp>
#include <queue>

using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::ColumnFamilyOptions;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::DBOptions;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

#define MESSAGE_LOG(lvl)\
    BOOST_LOG_STREAM_WITH_PARAMS(::boost::log::trivial::logger::get(), \
        (::boost::log::keywords::severity = lvl))

struct Arguments {
    std::string logLevelString;
    boost::log::trivial::severity_level logLevel;
    size_t threadCount;
    std::string output;
};

struct Value
{
  size_t handle_id;
  std::string key;
  std::string value;
};

class dbEditor {
 public:
  explicit dbEditor(std::string path, Arguments _arguments);
  void addValue(std::string tableName, std::string key, std::string value);
  void showTable(std::string name, std::string path) const;
  void showAllTables(const std::string& path) const;
  void createTable(std::string name);
  void hashDataBaseInit();
  void createTables(std::vector<ColumnFamilyDescriptor>* tables, DB* outputDb,
                    std::vector<ColumnFamilyHandle*> outputHandles);

  static void readRequest(size_t id, std::vector<ColumnFamilyHandle*>&
      inputHandles, DB* inputDb, std::mutex& _mutex, std::queue<Value>& _values,
                          size_t threadsNum);
  static void writeRequest(std::vector<ColumnFamilyHandle*>& outputHandles,
                           DB* outputDB, std::mutex& _mutex,
                           std::queue<Value>& _values);

 private:
  Arguments arguments;
  std::string db_path;
  Options options;
  std::mutex mutex;
  std::queue<Value> values;

  std::vector<ColumnFamilyDescriptor>* getTables(std::string name,
                                                 size_t& position) const;
  std::vector<ColumnFamilyDescriptor>* getTables(std::string path);
};

auto example() -> void;

#endif // INCLUDE_KV_STORAGE_HPP_
