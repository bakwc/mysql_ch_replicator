cmake_minimum_required(VERSION 3.20)

project(binlog_json_parser)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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
    else()
        message(WARNING "The -march option will not be set because the system is not x86 or x64.")
    endif()

    # Check for LTO support
    include(CheckCXXCompilerFlag)

    check_cxx_compiler_flag("-flto" COMPILER_SUPPORTS_LTO)

    if(COMPILER_SUPPORTS_LTO)
        message(STATUS "Link Time Optimization (LTO) is supported by the compiler.")
        set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -flto")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -flto")
    else()
        message(WARNING "Link Time Optimization (LTO) is not supported by the compiler.")
    endif()

    # Export compile flags to a file
    file(WRITE "${CMAKE_BINARY_DIR}/compile_flags.txt" "CXXFLAGS: ${CMAKE_CXX_FLAGS_RELEASE}\n")
    file(APPEND "${CMAKE_BINARY_DIR}/compile_flags.txt" "LINKER_FLAGS: ${CMAKE_EXE_LINKER_FLAGS}\n")

endif()

add_library(mysqljsonparse SHARED mysqljsonparse.cpp mysql_json_parser.cpp)
