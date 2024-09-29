cmake_minimum_required(VERSION 3.10)
project(clickhouse_data_processing)

set(CMAKE_CXX_STANDARD 14)

# Пути к библиотеке clickhouse-cpp
include_directories(pathtoclickhouse-cppinclude)
link_directories(pathtoclickhouse-cpplib)

# Добавьте исходные файлы проекта
add_executable(clickhouse_data_processing main.cpp data_reader.cpp data_processor.cpp)

# Линковка с библиотеками ClickHouse C++
target_link_libraries(clickhouse_data_processing clickhouse-cpp-lib-static)
