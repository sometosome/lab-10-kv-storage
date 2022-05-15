#include <kv-storage.hpp>

int main([[maybe_unused]]int argc, [[maybe_unused]]char** argv) {
    Arguments arguments = {"info", boost::log::trivial::error, 1, "../result"};
    boost::program_options::options_description desc("Parcer");
    desc.add_options()("help", "produce help message")(
            "log-level", boost::program_options::value<std::string>(),
            "Enter log level")(
            "thread-count", boost::program_options::value<size_t>(),
            "Enter thread count")(
            "output", boost::program_options::value<std::string>(),
            "Enter name of output file");
    boost::program_options::variables_map vm;
    boost::program_options::store(
            boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);
    if (vm.count("help")) {
        std::cout << desc << "\n";
        return 1;
    }
    if (vm.count("log-level")) {
        arguments.logLevelString = vm["log-level"].as<std::string>();
        if (arguments.logLevelString == "info")
        {
          arguments.logLevel = boost::log::trivial::info;
        }
        else if (arguments.logLevelString == "warning")
        {
          arguments.logLevel = boost::log::trivial::warning;
        }
        else
        {
          arguments.logLevel = boost::log::trivial::error;
        }
    }
    if (vm.count("thread-count")) {
        arguments.threadCount = vm["thread-count"].as<size_t>();
        if ((arguments.threadCount > std::thread::hardware_concurrency()) || (arguments.threadCount <= 0))
            arguments.threadCount = std::thread::hardware_concurrency();
    }
    if (vm.count("output")) {
        arguments.output = vm["output"].as<std::string>();
        if (arguments.output.empty())
            arguments.output = "../";
    }

//    MESSAGE_LOG(arguments.logLevel) << "Done!\n";
    dbEditor editor("../db", arguments);
    //editor.showAllTables("../result");
    editor.hashDataBaseInit();
}
