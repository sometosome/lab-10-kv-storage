// Copyright 2021 Your Name <your_email>

#ifndef INCLUDE_EXAMPLE_HPP_
#define INCLUDE_EXAMPLE_HPP_

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

using FDescriptorContainer = std::vector<rocksdb::ColumnFamilyDescriptor>;

struct Arguments {
    std::string logLevel;
    size_t threadCount;
    std::string output;
};

auto example() -> void;

#endif // INCLUDE_EXAMPLE_HPP_
