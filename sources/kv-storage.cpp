// Copyright 2021 Your Name <your_email>

#include <stdexcept>

#include <kv-storage.hpp>

FDescriptorContainer getFamilyDescriptors(std::string path) {
    rocksdb::Options options;
    std::vector<std::string> family;
    FDescriptorContainer descriptors;
    rocksdb::Status status = rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(), path, &family);

    assert(status.ok()); //if 0 -> exit



    for (const std::string &familyName : family)
    {
        descriptors.emplace_back(familyName, rocksdb::ColumnFamilyOptions());

    }
    return descriptors;
}

rocksdb::Status open_database(const FDescriptorContainer& descriptors,
                              std::vector <rocksdb::ColumnFamilyHandle*> columns,
                              rocksdb::DB* db, std::string path)
{
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status =
            rocksdb::DB::Open(rocksdb::DBOptions(), path, descriptors, &columns, &db);
    return status;
}

std::string get_value(rocksdb::DB* db,
                      const std::string& key, bool* found) {
    std::string value = "";
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &value);
    *found = s.ok();
    return value;
}

void set_value(rocksdb::DB* db,
               const std::string& key, const std::string& value) {
    rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, value);
    if(!s.ok())
        throw std::runtime_error("_db->Put failed");
}

auto example() -> void {
  throw std::runtime_error("not implemented");
}
