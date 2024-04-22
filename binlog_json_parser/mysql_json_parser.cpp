#include <iostream>
#include <sstream>
#include <iomanip>
#include <limits>


#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wunused-const-variable"


#include "mysql_json_parser.h"
#include "my_byteorder.h"


constexpr char JSONB_TYPE_SMALL_OBJECT = 0x0;
constexpr char JSONB_TYPE_LARGE_OBJECT = 0x1;
constexpr char JSONB_TYPE_SMALL_ARRAY = 0x2;
constexpr char JSONB_TYPE_LARGE_ARRAY = 0x3;
constexpr char JSONB_TYPE_LITERAL = 0x4;
constexpr char JSONB_TYPE_INT16 = 0x5;
constexpr char JSONB_TYPE_UINT16 = 0x6;
constexpr char JSONB_TYPE_INT32 = 0x7;
constexpr char JSONB_TYPE_UINT32 = 0x8;
constexpr char JSONB_TYPE_INT64 = 0x9;
constexpr char JSONB_TYPE_UINT64 = 0xA;
constexpr char JSONB_TYPE_DOUBLE = 0xB;
constexpr char JSONB_TYPE_STRING = 0xC;
constexpr char JSONB_TYPE_OPAQUE = 0xF;

constexpr char JSONB_NULL_LITERAL = 0x0;
constexpr char JSONB_TRUE_LITERAL = 0x1;
constexpr char JSONB_FALSE_LITERAL = 0x2;

constexpr uint8_t SMALL_OFFSET_SIZE = 2;
constexpr uint8_t LARGE_OFFSET_SIZE = 4;
constexpr uint8_t KEY_ENTRY_SIZE_SMALL = 2 + SMALL_OFFSET_SIZE;
constexpr uint8_t KEY_ENTRY_SIZE_LARGE = 2 + LARGE_OFFSET_SIZE;
constexpr uint8_t VALUE_ENTRY_SIZE_SMALL = 1 + SMALL_OFFSET_SIZE;
constexpr uint8_t VALUE_ENTRY_SIZE_LARGE = 1 + LARGE_OFFSET_SIZE;


std::string parse_value(uint8_t type, const char* data, size_t len, size_t depth);


static uint8_t json_binary_key_entry_size(bool large) {
  return large ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL;
}

static uint8_t json_binary_value_entry_size(bool large) {
  return large ? VALUE_ENTRY_SIZE_LARGE : VALUE_ENTRY_SIZE_SMALL;
}

static uint32_t read_offset_or_size(const char *data, bool large) {
  return large ? uint4korr(data) : uint2korr(data);
}

static uint8_t json_binary_offset_size(bool large) {
  return large ? LARGE_OFFSET_SIZE : SMALL_OFFSET_SIZE;
}

static uint8_t offset_size(bool large) {
  return large ? LARGE_OFFSET_SIZE : SMALL_OFFSET_SIZE;
}

inline size_t value_entry_offset(size_t pos, bool is_object, bool m_large, size_t m_element_count) {
  size_t first_entry_offset = 2 * offset_size(m_large);
  if (is_object)
    first_entry_offset += m_element_count * json_binary_key_entry_size(m_large);

  return first_entry_offset + json_binary_value_entry_size(m_large) * pos;
}

inline size_t key_entry_offset(size_t pos, bool m_large) {
  // The first key entry is located right after the two length fields.
  return 2 * offset_size(m_large) + json_binary_key_entry_size(m_large) * pos;
}

static bool inlined_type(uint8_t type, bool large) {
  switch (type) {
    case JSONB_TYPE_LITERAL:
    case JSONB_TYPE_INT16:
    case JSONB_TYPE_UINT16:
      return true;
    case JSONB_TYPE_INT32:
    case JSONB_TYPE_UINT32:
      return large;
    default:
      return false;
  }
}

static bool read_variable_length(const char *data, size_t data_length,
                                 uint32_t *length, uint8_t *num) {
  /*
    It takes five bytes to represent UINT_MAX32, which is the largest
    supported length, so don't look any further.
  */
  const size_t max_bytes = std::min(data_length, static_cast<size_t>(5));

  size_t len = 0;
  for (size_t i = 0; i < max_bytes; i++) {
    // Get the next 7 bits of the length.
    len |= (data[i] & 0x7f) << (7 * i);
    if ((data[i] & 0x80) == 0) {
      // The length shouldn't exceed 32 bits.
      if (len > std::numeric_limits<uint32_t>::max()) return true; /* purecov: inspected */

      // This was the last byte. Return successfully.
      *num = static_cast<uint8_t>(i + 1);
      *length = static_cast<uint32_t>(len);
      return false;
    }
  }

  // No more available bytes. Return true to signal error.
  return true; /* purecov: inspected */
}


std::string escape_json(const std::string &s) {
  std::ostringstream o;
  for (auto c = s.cbegin(); c != s.cend(); c++) {
    switch (*c) {
      case '"': o << "\\\""; break;
      case '\\': o << "\\\\"; break;
      case '\b': o << "\\b"; break;
      case '\f': o << "\\f"; break;
      case '\n': o << "\\n"; break;
      case '\r': o << "\\r"; break;
      case '\t': o << "\\t"; break;
      default:
        if (*c <= '\x1f') {
          o << "\\u"
            << std::hex << std::setw(4) << std::setfill('0') << static_cast<int>(*c);
        } else {
          o << *c;
        }
    }
  }
  return o.str();
}


static std::string parse_scalar(uint8_t type, const char *data, size_t len, size_t depth) {
  (void)(depth);

  switch (type) {
    case JSONB_TYPE_LITERAL:
      if (len < 1) {
        throw std::runtime_error("invalid len");
      }
      switch (static_cast<uint8_t>(*data)) {
        case JSONB_NULL_LITERAL:
          return "null";
        case JSONB_TRUE_LITERAL:
          return "true";
        case JSONB_FALSE_LITERAL:
          return "false";
        default:
          throw std::runtime_error("unknown literal");
      }
    case JSONB_TYPE_INT16:
      if (len < 2) {
        throw std::runtime_error("invalid len");
      }
      return std::to_string(sint2korr(data));
    case JSONB_TYPE_INT32:
      if (len < 4) {
        throw std::runtime_error("invalid len");
      }
      return std::to_string(sint4korr(data));
    case JSONB_TYPE_INT64:
      if (len < 8) {
        throw std::runtime_error("invalid len");
      }
      return std::to_string(sint8korr(data));
    case JSONB_TYPE_UINT16:
      if (len < 2) {
        throw std::runtime_error("invalid len");
      }
      return std::to_string(uint2korr(data));
    case JSONB_TYPE_UINT32:
      if (len < 4) {
        throw std::runtime_error("invalid len");
      }
      return std::to_string(uint4korr(data));
    case JSONB_TYPE_UINT64:
      if (len < 8) {
        throw std::runtime_error("invalid len");
      }
      return std::to_string(uint8korr(data));
    case JSONB_TYPE_DOUBLE: {
      if (len < 8) {
        throw std::runtime_error("invalid len");
      }
      return std::to_string(float8get(data));
    }
    case JSONB_TYPE_STRING: {
      uint32_t str_len;
      uint8_t n;
      if (read_variable_length(data, len, &str_len, &n)) {
        throw std::runtime_error("failed to read len");
      }
      if (len < n + str_len) {
        throw std::runtime_error("invalid len");
      }
      std::string result;
      result += '"';
      result += escape_json(std::string(data + n, str_len));
      result += '"';
      return result;
    }
      //        case JSONB_TYPE_OPAQUE: {
      //            /*
      //              There should always be at least one byte, which tells the field
      //              type of the opaque value.
      //            */
      //            if (len < 1) return err(); /* purecov: inspected */
      //
      //            // The type is encoded as a uint8_t that maps to an enum_field_types.
      //            const uint8_t type_byte = static_cast<uint8_t>(*data);
      //            const enum_field_types field_type =
      //                    static_cast<enum_field_types>(type_byte);
      //
      //            // Then there's the length of the value.
      //            uint32_t val_len;
      //            uint8_t n;
      //            if (read_variable_length(data + 1, len - 1, &val_len, &n))
      //                return err();                          /* purecov: inspected */
      //            if (len < 1 + n + val_len) return err(); /* purecov: inspected */
      //            return Value(field_type, data + 1 + n, val_len);
      //        }
    default:
      // Not a valid scalar type.
      throw std::runtime_error("invalid scalar type");
  }
}

std::string get_element(
    size_t pos, size_t m_element_count, size_t m_length,
    bool m_large, const char *m_data, bool is_object, size_t depth
) {

  if (pos >= m_element_count) {
    throw std::runtime_error("out of array");
  }

  const auto entry_size = json_binary_value_entry_size(m_large);
  const auto entry_offset = value_entry_offset(pos, is_object, m_large, m_element_count);

  const uint8_t type = m_data[entry_offset];

  /*
    Check if this is an inlined scalar value. If so, return it.
    The scalar will be inlined just after the byte that identifies the
    type, so it's found on entry_offset + 1.
  */
  if (inlined_type(type, m_large)) {
    return parse_scalar(type, m_data + entry_offset + 1, entry_size - 1, depth);
  }

  /*
    Otherwise, it's a non-inlined value, and the offset to where the value
    is stored, can be found right after the type byte in the entry.
  */
  const uint32_t value_offset =
      read_offset_or_size(m_data + entry_offset + 1, m_large);

  if (m_length < value_offset || value_offset < entry_offset + entry_size) {
    throw std::runtime_error("wrong offset");
  }

  return parse_value(type, m_data + value_offset, m_length - value_offset, depth);
}

std::string get_key(
    size_t pos, size_t m_element_count, size_t m_length,
    bool m_large, const char *m_data, bool is_object
) {
//    assert(is_object);
  (void)(is_object);

  if (pos >= m_element_count) {
    throw std::runtime_error("wrong position");
  }

  const auto offset_size = json_binary_offset_size(m_large);
  const auto key_entry_size = json_binary_key_entry_size(m_large);
  const auto value_entry_size = json_binary_value_entry_size(m_large);

  // The key entries are located after two length fields of size offset_size.
  const size_t entry_offset = key_entry_offset(pos, m_large);

  // The offset of the key is the first part of the key entry.
  const uint32_t key_offset = read_offset_or_size(m_data + entry_offset, m_large);

  // The length of the key is the second part of the entry, always two bytes.
  const uint16_t key_length = uint2korr(m_data + entry_offset + offset_size);

  /*
    The key must start somewhere after the last value entry, and it must
    end before the end of the m_data buffer.
  */
  if ((key_offset < entry_offset + (m_element_count - pos) * key_entry_size +
                    m_element_count * value_entry_size) ||
      (m_length < key_offset + key_length)
      ) {
    throw std::runtime_error("wrong key position");
  }

  std::string result;
  result += '"';
  result += std::string(m_data + key_offset, key_length);
  result += '"';

  return result;
}


std::string parse_array_or_object(bool is_object, const char *data,
                                  size_t len, bool large, size_t depth)
{
  const auto offset_size = json_binary_offset_size(large);
  if (len < 2 * offset_size) {
    throw std::runtime_error("length is too big");
  }
  const uint32_t element_count = read_offset_or_size(data, large);
  const uint32_t bytes = read_offset_or_size(data + offset_size, large);

  // The value can't have more bytes than what's available in the data buffer.
  if (bytes > len) {
    throw std::runtime_error("length is too big");
  }

  /*
    Calculate the size of the header. It consists of:
    - two length fields
    - if it is a JSON object, key entries with pointers to where the keys
      are stored
    - value entries with pointers to where the actual values are stored
  */
  size_t header_size = 2 * offset_size;
  if (is_object) {
    header_size += element_count * json_binary_key_entry_size(large);
  }
  header_size += element_count * json_binary_value_entry_size(large);

  // The header should not be larger than the full size of the value.
  if (header_size > bytes) {
    throw std::runtime_error("header size overflow");
  }

  if (element_count == 0) {
    if (is_object) {
      return "{}";
    } else {
      return "[]";
    }
  }

  std::string result;

  if (is_object) {
    //        result += "{\n";
    result += "{";
  } else {
    //        result += "[\n";
    result += "[";
  }

  for (size_t i = 0; i < element_count; ++i) {
    for (size_t d = 0; d < depth + 1; ++d) {
      //            result += "  ";
    }
    std::string element = get_element(
        i, element_count, bytes, large, data, is_object, depth + 1
    );
    if (is_object) {
      std::string key = get_key(
          i, element_count, bytes, large, data, is_object
      );
      result += key;
      result += ": ";
      result += element;
    } else {
      result += element;
    }

    if (i < element_count - 1) {
      //            result += ",\n";
      result += ", ";
    } else {
      //            result += "\n";
    }
  }

  for (size_t d = 0; d < depth; ++d) {
    //        result += "  ";
  }

  if (is_object) {
    result += "}";
  } else {
    result += "]";
  }

  return result;
}

std::string parse_value(uint8_t type, const char* data, size_t len, size_t depth) {
  switch (type) {
    case JSONB_TYPE_SMALL_OBJECT:
      return parse_array_or_object(true, data, len, false, depth);
    case JSONB_TYPE_LARGE_OBJECT:
      return parse_array_or_object(true, data, len, true, depth);
    case JSONB_TYPE_SMALL_ARRAY:
      return parse_array_or_object(false, data, len, false, depth);
    case JSONB_TYPE_LARGE_ARRAY:
      return parse_array_or_object(false, data, len, true, depth);
    default:
      return parse_scalar(type, data, len, depth);
  }
}

std::string parse_mysql_json(const char* data, size_t len) {
  if (len == 0) {
    return "null";
  }
  return parse_value(data[0], data+1, len-1, 0);
}

#pragma clang diagnostic pop
