#include <iostream>
#include <string>
#include "mysql_json_parser.h"

extern "C" {
  void test_func();
  const char* test_str_func(const char* str, size_t size);
  const char* mysql_to_json(const char* str, size_t size);
}

void test_func() {
  std::cout << " === test_func output ===\n";
}

const char* test_str_func(const char* str, size_t size) {
  std::cout << std::string(str, size) << "\n";
  return " === test_str_func return result ===";
}

std::string last_call_result;
const char* mysql_to_json(const char* str, size_t size) {
  last_call_result = parse_mysql_json(str, size);
  return last_call_result.c_str();
}
