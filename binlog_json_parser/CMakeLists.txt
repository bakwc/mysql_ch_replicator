cmake_minimum_required(VERSION 3.0)
project(binlog_json_parser)

set(CMAKE_CXX_STANDARD 23)

#add_executable(binlog_json_parser main.cpp mysql_json_parser.cpp)
add_library(mysqljsonparse SHARED mysqljsonparse.cpp mysql_json_parser.cpp)
