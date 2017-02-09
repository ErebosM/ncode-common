cmake_minimum_required(VERSION 2.8.7)

project(NCodeCommon)
include(CTest)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake_extensions)

if (CMAKE_VERSION VERSION_LESS 3.2)
    set(UPDATE_DISCONNECTED_IF_AVAILABLE "")
else()
    set(UPDATE_DISCONNECTED_IF_AVAILABLE "UPDATE_DISCONNECTED 1")
endif()

# Set a default build type if none was specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
   message(STATUS "Setting build type to 'Debug' as none was specified.")
   set(CMAKE_BUILD_TYPE Debug CACHE STRING "Choose the type of build." FORCE)
   # Set the possible values of build type for cmake-gui
   set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release")
endif()

set(ASAN FALSE CACHE BOOL "Compile with ASAN on")
set(TSAN FALSE CACHE BOOL "Compile with TSAN on")

set(BASE_FLAGS "-g -std=c++11 -pedantic-errors -Winit-self -Woverloaded-virtual -Wuninitialized -Wall -Wextra -fno-exceptions")
set(BASE_LD_FLAGS "")
if (ASAN)
   set(BASE_FLAGS "${BASE_FLAGS} -fsanitize=address -fno-omit-frame-pointer -fno-optimize-sibling-calls")
   set(BASE_LD_FLAGS "${BASE_LD_FLAGS} -fsanitize=address")
endif()
if (TSAN)
   set(BASE_FLAGS "${BASE_FLAGS} -fsanitize=thread -fno-omit-frame-pointer -fno-optimize-sibling-calls")
   set(BASE_LD_FLAGS "${BASE_LD_FLAGS} -fsanitize=thread")
endif()

if(APPLE)
elseif(UNIX)
  set(BASE_FLAGS "${BASE_FLAGS} -pthread")
endif()

set(BASE_DEBUG_FLAGS "${BASE_FLAGS} -O0 -fno-omit-frame-pointer --coverage")
set(BASE_OPT_FLAGS "${BASE_FLAGS} -O3 -march=native -DNDEBUG")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${BASE_DEBUG_FLAGS}")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${BASE_OPT_FLAGS}")
set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} ${BASE_LD_FLAGS} --coverage")
set(CMAKE_LINKER_FLAGS_RELEASE "${CMAKE_LINKER_FLAGS_RELEASE} ${BASE_LD_FLAGS}")

add_subdirectory(external/gtest)

macro(add_test_exec name src_file deps)
  add_executable(${name} ${src_file})
  target_link_libraries(${name} gtest gmock_main ${deps} ${ARGN})
  add_test(NAME ${name} COMMAND ${name})
endmacro(add_test_exec)  

################################
# Common stuff
################################
set(COMMON_HEADER_FILES src/common.h src/substitute.h src/logging.h src/file.h src/stringpiece.h src/strutil.h src/map_util.h src/stl_util.h src/event_queue.h src/free_list.h src/packer.h src/ptr_queue.h src/lru_cache.h src/perfect_hash.h src/alphanum.h src/md5.h)
add_library(ncode_common STATIC src/common.cc src/substitute.cc src/logging.cc src/file.cc src/stringpiece.cc src/strutil.cc src/event_queue.cc src/packer.cc src/md5.cc ${COMMON_HEADER_FILES})

set_property(SOURCE src/stringpiece_test.cc APPEND_STRING PROPERTY COMPILE_FLAGS "-Wno-conversion-null -Wno-sign-compare")
set_property(SOURCE src/strutil_test.cc APPEND_STRING PROPERTY COMPILE_FLAGS "-Wno-sign-compare")
add_test_exec(stringpiece_test src/stringpiece_test.cc ncode_common)
add_test_exec(logging_test src/logging_test.cc ncode_common)
add_test_exec(common_test src/common_test.cc ncode_common)
add_test_exec(strutil_test src/strutil_test.cc ncode_common)
add_test_exec(event_queue_test src/event_queue_test.cc ncode_common)
add_test_exec(free_list_test src/free_list_test.cc ncode_common)
add_test_exec(packer_test src/packer_test.cc ncode_common)
add_test_exec(ptr_queue_test src/ptr_queue_test.cc ncode_common)
add_test_exec(circular_array_test src/circular_array_test.cc ncode_common)
add_test_exec(lru_cache_test src/lru_cache_test.cc ncode_common)
add_test_exec(thread_runner_test src/thread_runner_test.cc ncode_common)
add_test_exec(perfect_hash_test src/perfect_hash_test.cc ncode_common)
add_test_exec(alphanum_test src/alphanum_test.cc ncode_common)

add_executable(perfect_hash_benchmark src/perfect_hash_benchmark.cc)
target_link_libraries(perfect_hash_benchmark ncode_common)
