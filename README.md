[![Build Status](https://travis-ci.org/ngvozdiev/ncode-common.svg?branch=master)](https://travis-ci.org/ngvozdiev/ncode-common)

# What is NCode?

NCode is a small C++11 library that extends STL's functionality with various odds and ends that I find useful. Emphasis is on code clarity and simplicity (no complicated template code). It also contains reusable bits of other projects (mostly protobufs).

## Installation

Assuming that your project's external dependencies are in `<project_root>/external/` you can create a new directory `external/ncode/ncode-common` and clone this repo there. Alternatively you can add it as a github submodule.

Here is a sample CMakeLists.txt file that shows how to include NCode into your build if you have a single file `src/main.cc`:

    cmake_minimum_required(VERSION 2.8.7)

    add_subdirectory(external/ncode/ncode-common)
    include_directories(external/ncode)

    set(CMAKE_CXX_FLAGS "-std=c++11 -Wall -Wextra")
    add_executable(main src/main.cc)
    target_link_libraries(main ncode_common) 

In `main.cc` you can include as:

`#include "ncode-common/src/common.h"`