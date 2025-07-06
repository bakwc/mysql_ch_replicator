cmake_minimum_required(VERSION 3.20)

project(binlog_json_parser)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(CheckIPOSupported)
include(CheckCSourceCompiles)
check_ipo_supported(RESULT IPO_SUPPORTED OUTPUT IPO_OUTPUT)

if(IPO_SUPPORTED)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    message(STATUS "Interprocedural optimization (IPO/LTO) enabled globally.")
else()
    message(STATUS "IPO/LTO is not supported: ${IPO_OUTPUT}")
endif()

# Check if the build type is Release
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    # Set optimization level to -O3 for release builds
    if(NOT CMAKE_CXX_FLAGS_RELEASE MATCHES "-O")
        set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
    endif()

    # Option to add march-native for release builds
    option(USE_MARCH_NATIVE "Enable -march=native for release builds" OFF)

    # Determine the architecture
    include(CMakeDetermineSystem)

    if(CMAKE_SYSTEM_PROCESSOR MATCHES "86")
        if(USE_MARCH_NATIVE)
            set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native")
        else()
            # Set default march flag to skylake (2015 year) if not using native
            if(NOT CMAKE_CXX_FLAGS_RELEASE MATCHES "march=")
                set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=skylake")
            endif()
        endif()
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64|ARM64")
        if(USE_MARCH_NATIVE)
            set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native")
        else()
            if(NOT CMAKE_CXX_FLAGS_RELEASE MATCHES "march=")
                set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=armv8.2-a")
            endif()
        endif()
    else()
        message(WARNING "The -march option will not be set because the system is not x86, x64, or ARM64.")
    endif()

    # Export compile flags to a file
    file(WRITE "${CMAKE_BINARY_DIR}/compile_flags.txt" "CXXFLAGS: ${CMAKE_CXX_FLAGS_RELEASE}\n")
    file(APPEND "${CMAKE_BINARY_DIR}/compile_flags.txt" "LINKER_FLAGS: ${CMAKE_EXE_LINKER_FLAGS}\n")
endif()

check_c_source_compiles("
#include <features.h>
#if defined(__GLIBC__)
#error \"This is glibc, not musl\"
#endif
#include <stdio.h>
int main() { return 0; }
" IS_MUSL)

option(ALPINE_STATIC "Force fully static build when using musl/Alpine" ${IS_MUSL})

if(ALPINE_STATIC)
    add_definitions(-D_FORTIFY_SOURCE=0)
    message(STATUS "musl detected → producing shared library with static musl linking")
    
    add_library(mysqljsonparse SHARED mysqljsonparse.cpp mysql_json_parser.cpp)
    target_link_options(mysqljsonparse PRIVATE 
        -static
        -static-libgcc 
        -static-libstdc++
        -fPIC
    )
    target_compile_options(mysqljsonparse PRIVATE -fPIC)
else()
    message(STATUS "musl not detected → building shared library with dynamic glibc")
    add_library(mysqljsonparse SHARED mysqljsonparse.cpp mysql_json_parser.cpp)
endif()
