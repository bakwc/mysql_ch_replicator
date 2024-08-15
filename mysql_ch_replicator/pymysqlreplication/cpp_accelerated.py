import platform
import ctypes
from ctypes import c_int, c_char_p
import os
import platform

MODULE_DIR = os.path.dirname(__file__)

FILE_NAME = 'libmysqljsonparse'
FILE_PATH = None

if platform.system() == 'Darwin':
    FILE_PATH = f'{FILE_NAME}.dylib'
if platform.system() == 'Linux':
    FILE_PATH = f'{FILE_NAME}.so'
    if platform.machine() == 'x86_64':
        FILE_PATH = f'{FILE_NAME}_x86_64.so'

FILE_PATH = os.path.join(MODULE_DIR, FILE_PATH)

lib = ctypes.cdll.LoadLibrary(FILE_PATH)

test_func = lib.test_func
test_func.argtypes = ()
test_func.restype = None

test_str_func = lib.test_str_func
test_str_func.argtypes = (c_char_p,c_int)
test_str_func.restype = c_char_p

mysql_to_json = lib.mysql_to_json
mysql_to_json.argtypes = (c_char_p,c_int)
mysql_to_json.restype = c_char_p


def cpp_mysql_to_json(data: bytes) -> bytes:
    return mysql_to_json(c_char_p(data), c_int(len(data)))
