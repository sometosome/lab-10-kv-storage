// Copyright 2021 Your Name <your_email>

#include <stdexcept>
#include <kv-storage.hpp>

dbEditor::dbEditor(std::string path, Arguments _arguments) {
  this->db_path = path;
  this->options.create_if_missing = true;
  this->options.create_missing_column_families = true;
  this->options.keep_log_file_num = 1;
  this->options.info_log_level = ::rocksdb::InfoLogLevel::FATAL_LEVEL;
  this->options.recycle_log_file_num = 1;
  this->arguments.logLevel = _arguments.logLevel;
  this->arguments.output = _arguments.output;
  this->arguments.threadCount = _arguments.threadCount;

  DB* db;
  Status status;
  status = rocksdb::DB::Open(options, this->db_path, &db);
  if (db)
  {
    delete db;
  }
  status = rocksdb::DB::Open(options, this->arguments.output, &db);
  if (db)
  {
    delete db;
  }
}

std::vector<ColumnFamilyDescriptor>* dbEditor::getTables
    (std::string name, size_t& position) const {
  auto* column_families = new std::vector<ColumnFamilyDescriptor>;
  auto* column_names = new std::vector<std::string>;
  bool already_in_list = name == "";
  size_t i = 0;
  Status status;

  status = DB::ListColumnFamilies(this->options, this->db_path, column_names);
  if (!status.ok())
  {
    std::cerr << status.ToString() << std::endl;
  }
  assert(status.ok());

  for (std::string temp : *column_names)
  {
    if (temp == name)
    {
      already_in_list = true;
      position = i;
    }
    column_families->emplace_back(temp, ColumnFamilyOptions());
    i++;
  }
  if (!already_in_list)
  {
    position = i;
    column_families->emplace_back(name, ColumnFamilyOptions());
  }

  if (column_names)
  {
    delete column_names;
  }
  return column_families;
}

std::vector<ColumnFamilyDescriptor>* dbEditor::getTables(std::string path) {
  auto* column_families = new std::vector<ColumnFamilyDescriptor>();
  auto* column_names = new std::vector<std::string>();
  Status status;

  status = DB::ListColumnFamilies(options, path, column_names);
  if (status.IsIOError())
  {
    column_families->emplace_back(ROCKSDB_NAMESPACE::kDefaultColumnFamilyName,
                                  ColumnFamilyOptions());
  }

  if (!status.ok() && status.IsIOError())
  {
    std::cerr << status.ToString() << std::endl;
  }
  assert(status.ok());

  for (std::string name : *column_names)
    column_families->emplace_back(name, ColumnFamilyOptions());
  if (column_families)
  {
    delete column_names;
  }
  return column_families;
}

void CleanUp(std::vector<ColumnFamilyHandle*>& handles, std::vector<ColumnFamilyDescriptor>* column_families, DB* db) {
  for (auto handle : handles)
  {
    db->DestroyColumnFamilyHandle(handle);
  }
  if (column_families)
  {
    delete column_families;
  }
  if (db)
  {
    delete db;
  }
}

void dbEditor::addValue(std::string tableName,
                        std::string key,
                        std::string value) {
  DB* db;
  std::vector<ColumnFamilyHandle*> handles;
  Status status;

  if (tableName == "0")
  {
    tableName = ROCKSDB_NAMESPACE::kDefaultColumnFamilyName;
  }
  size_t position;
  auto* column_families = getTables(tableName, position);
  status = DB::Open(DBOptions(), db_path, *column_families, &handles, &db);
  if (!status.ok())
  {
    std::cerr << status.ToString() << std::endl;
    CleanUp(handles, column_families, db);
  }
  assert(status.ok());

  db->Put(WriteOptions(), handles[position], key, value);
  CleanUp(handles, column_families, db);
}

void dbEditor::showTable(std::string name, std::string path) const {
  DB* db;
  size_t position;
  auto* column_families = getTables(name, position);
  std::vector<ColumnFamilyHandle*> handles;
  Status status;

  status = DB::Open(DBOptions(), path, *column_families, &handles, &db);
  if (!status.ok())
  {
    std::cerr << status.ToString() << std::endl;
  }
  assert(status.ok());

  std::cout << "Table name: " << name << std::endl;
  rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions(),
                                          handles[position]);
  for (it->SeekToFirst(); it->Valid(); it->Next())
  {
    std::cout << "\t" << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }

  for (auto & handle : handles)
  {
    db->DestroyColumnFamilyHandle(handle);
  }
  if (it)
  {
    delete it;
  }
  if (column_families)
  {
    delete column_families;
  }
  if (db)
  {
    delete db;
  }
}

void dbEditor::showAllTables(const std::string& path) const  {
  size_t position;
  auto* column_families = getTables("", position);

  for (auto& column_familie : *column_families)
  {
    showTable(column_familie.name, path);
  }

  if (column_families)
  {
    delete column_families;
  }
}

void dbEditor::createTable(std::string name) {
  DB* db;
  size_t position;
  auto* column_families = getTables("", position);
  std::vector<ColumnFamilyHandle*> handles;
  Status status;
  ColumnFamilyHandle* cf;

  status = DB::Open(DBOptions(), db_path, *column_families, &handles, &db);
  if (!status.ok())
  {
    std::cerr << status.ToString() << std::endl;
  }
  assert(status.ok());

  status = db->CreateColumnFamily(ColumnFamilyOptions(), name, &cf);
  if (!status.ok())
  {
    std::cerr << status.ToString() << std::endl;
  }
  assert(status.ok());

  for (auto handle : handles)
  {
    db->DestroyColumnFamilyHandle(handle);
  }
  if (cf)
  {
    delete cf;
  }
  if (column_families)
  {
    delete column_families;
  }
  if (db)
  {
    delete db;
  }
}

void dbEditor::createTables(std::vector<ColumnFamilyDescriptor>* tables,
                            DB* outputDb,
                            std::vector<ColumnFamilyHandle*> outputHandles) {
  for (size_t i = 1; i != tables->size(); i++) {
    ColumnFamilyHandle* cf;
    Status s = outputDb->CreateColumnFamily(ColumnFamilyOptions(),
                                               (*tables)[i].name, &cf);
    outputHandles.push_back(cf);
    if (!s.ok())
    {
      if (this->arguments.logLevel == boost::log::trivial::error)
      {
        MESSAGE_LOG(this->arguments.logLevel)
            << (*tables)[i].name << ": " << s.ToString() << std::endl;
      }
    }
    assert(s.ok());
  }
}

void dbEditor::readRequest(size_t id,
                           std::vector<ColumnFamilyHandle*>& inputHandles,
                           DB* inputDb,
                           std::mutex& _mutex,
                           std::queue<Value>& _values,
                           size_t threadsNum) {
  for (size_t i = id; i < inputHandles.size(); i += threadsNum)
  {
    rocksdb::Iterator* it =
        inputDb->NewIterator(rocksdb::ReadOptions(), inputHandles[i]);
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
      _mutex.lock();
      _values.push({ i, it->key().ToString(), it->value().ToString() });
      _mutex.unlock();
    }
    if (it)
    {
      delete it;
    }
  }
}

void dbEditor::writeRequest(std::vector<ColumnFamilyHandle*>& outputHandles,
                            DB* outputDB,
                            std::mutex& _mutex,
                            std::queue<Value>& _values) {
  while (!_values.empty())
  {
    _mutex.lock();
    auto pair = _values.front();
    _values.pop();
    _mutex.unlock();
    std::string hash;
    picosha2::hash256_hex_string(pair.key + pair.value, hash);
    outputDB->Put(WriteOptions(), outputHandles[pair.handle_id], pair.key,
                     hash);
  }
}

void dbEditor::hashDataBaseInit() {
  auto inputTables = getTables(this->db_path);
  auto outputTables = getTables(this->arguments.output);
  std::vector<ColumnFamilyHandle*> inputHandles;
  std::vector<ColumnFamilyHandle*> outputHandles;
  DB* inputDb;
  DB* outputDb;
  Status status;

  if (outputTables->size() > 1)
  {
    if (this->arguments.logLevel == boost::log::trivial::error)
    {
      MESSAGE_LOG(this->arguments.logLevel)
          << "Database already exists, clean directory and try again\n";
      return;
    }
  }
  if (outputTables->size() <= 1)
  {
    outputTables = new std::vector<ColumnFamilyDescriptor>(*inputTables);
    status = DB::Open(options, this->db_path, *inputTables, &inputHandles,
                 &inputDb);
    if (!status.ok())
    {
      if (this->arguments.logLevel == boost::log::trivial::error)
      {
        MESSAGE_LOG(this->arguments.logLevel) << status.ToString() << std::endl;
      }
    }
    assert(status.ok());
    status = DB::Open(options, this->arguments.output,
                     *outputTables, &outputHandles, &outputDb);
    if (!status.ok())
    {
      if (this->arguments.logLevel == boost::log::trivial::error)
      {
        MESSAGE_LOG(this->arguments.logLevel) << status.ToString() << std::endl;
      }
    }
    assert(status.ok());

    //producer
    if (this->arguments.logLevel == boost::log::trivial::info)
    {
      MESSAGE_LOG(this->arguments.logLevel) << "Reading input database";
    }
    for (size_t i = 0; i != this->arguments.threadCount; ++i)
    {
      std::thread thread(&dbEditor::readRequest, i,
                         std::ref(inputHandles),
                         inputDb, std::ref(this->mutex),
                         std::ref(this->values),
                         this->arguments.threadCount);
      if (thread.joinable())
      {
        thread.join();
      }
    }

    //consumer
    if (this->arguments.logLevel == boost::log::trivial::info)
    {
      MESSAGE_LOG(this->arguments.logLevel)
          << "Writing results to output database";
    }
    for (size_t i = 0; i != this->arguments.threadCount; ++i)
    {
      std::thread thread(&dbEditor::writeRequest,
                         std::ref(outputHandles),
                         outputDb,
                         std::ref(this->mutex),
                         std::ref(this->values));
      if (thread.joinable())
      {
        thread.join();
      }
    }

    for (auto handle : inputHandles)
      inputDb->DestroyColumnFamilyHandle(handle);
    for (auto handle : outputHandles)
      outputDb->DestroyColumnFamilyHandle(handle);
    if (outputDb)
    {
      delete outputDb;
    }
    if (inputDb)
    {
      delete inputDb;
    }

    if (this->arguments.logLevel == boost::log::trivial::info)
    {
      MESSAGE_LOG(this->arguments.logLevel) << "Input DB:";
      this->showAllTables(this->db_path);
      MESSAGE_LOG(this->arguments.logLevel) << "Output DB:";
      this->showAllTables(this->arguments.output);
    }
  }
}

auto example() -> void {
  throw std::runtime_error("not implemented");
}
