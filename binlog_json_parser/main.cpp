#include <iostream>

#include "mysql_json_parser.h"

int main() {

  std::string data_raw = {0x0,0x1,0x0,0x26,0x0,0xb,0x0,0x3,0x0,0x0,0xe,0x0,0x66,0x6f,0x6f,0x2,0x0,0x18,0x0,0x12,0x0,0x3,0x0,0x15,0x0,0x3,0x0,0x5,0xa,0x0,0x5,0x16,0x0,0x62,0x61,0x72,0x6b,0x72,0x6f};

  std::string result = parse_mysql_json(data_raw.data(), data_raw.size());
  std::cout << result << std::endl;

  return 0;
}
