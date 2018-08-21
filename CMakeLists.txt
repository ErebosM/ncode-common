cmake_minimum_required(VERSION 2.8.7)

project(NCode)

if (APPLE)
   set(CMAKE_MACOSX_RPATH 1)
endif()

option(NCODE_DISABLE_TESTS "If tests should be compiled or not" OFF)
option(NCODE_DISABLE_BENCHMARKS "If benchmarks should be compiled or not" OFF)
option(NCODE_ASAN "Compile with ASAN on" OFF)
option(NCODE_TSAN "Compile with TSAN on" OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'Release' as none was specified.")
  set(CMAKE_BUILD_TYPE "Release" CACHE
      STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release")
endif()

set(NCODE_BASE_FLAGS "-g -std=c++11 -pedantic-errors -Winit-self -Woverloaded-virtual -Wuninitialized -Wall -Wextra -fno-exceptions -D_FILE_OFFSET_BITS=64")
set(NCODE_BASE_LD_FLAGS "")
if (UNIX)
  if (NOT APPLE)
    set(NCODE_BASE_FLAGS "${NCODE_BASE_FLAGS} -fPIC -pthread")
    set(NCODE_BASE_LD_FLAGS "${NCODE_BASE_LD_FLAGS} -pthread")
  endif()
endif()

if (NCODE_ASAN)
   set(NCODE_BASE_FLAGS "${NCODE_BASE_FLAGS} -fsanitize=address -fno-omit-frame-pointer -fno-optimize-sibling-calls")
   set(NCODE_BASE_LD_FLAGS "${NCODE_BASE_LD_FLAGS} -fsanitize=address")
endif()
if (NCODE_TSAN)
   set(NCODE_BASE_FLAGS "${NCODE_BASE_FLAGS} -fsanitize=thread -fno-omit-frame-pointer -fno-optimize-sibling-calls")
   set(NCODE_BASE_LD_FLAGS "${NCODE_BASE_LD_FLAGS} -fsanitize=thread")
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${NCODE_BASE_FLAGS} -O0 -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${NCODE_BASE_FLAGS} -O3 -march=native -DNDEBUG")
set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} ${NCODE_BASE_LD_FLAGS}")

if (NOT NCODE_DISABLE_TESTS)
   include(CTest)
   add_subdirectory(external/gtest EXCLUDE_FROM_ALL)
   macro(add_test_exec name src_file deps)
     add_executable(${name} ${src_file})
     target_link_libraries(${name} gtest gmock_main ${deps} ${ARGN})
     add_test(NAME ${name} COMMAND ${name})
   endmacro(add_test_exec)
endif()

# Need PCAP for the interface in src/net/pcap.cc
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake_extensions)
find_package(PCAP REQUIRED)
include_directories(${PCAP_INCLUDE_DIR})

# Creates C resources file from files in given directory
function(create_resources dir output prefix)
  # Create empty output file
  file(WRITE ${output} "")
  # Collect input files
  file(GLOB bins ${dir}/*)
  # Iterate through input files
  foreach(bin ${bins})
    # Get short filename
    string(REGEX MATCH "([^/]+)$" filename ${bin})
    # Replace filename spaces & extension separator for C compatibility
    string(REGEX REPLACE "\\.| |-" "_" filename ${filename})
    # Read hex data from file
    file(READ ${bin} filedata HEX)
    # Convert hex data for C compatibility
    string(REGEX REPLACE "([0-9a-f][0-9a-f])" "0x\\1," filedata ${filedata})
    # Append data to output file
    file(APPEND ${output} "const unsigned char ${prefix}_${filename}[] = {${filedata}};\nconst unsigned ${prefix}_${filename}_size = sizeof(${prefix}_${filename});\n")
  endforeach()
endfunction()

create_resources("${PROJECT_SOURCE_DIR}/data/www" "${PROJECT_BINARY_DIR}/www_resources.c" "www")
create_resources("${PROJECT_SOURCE_DIR}/data/grapher_templates" "${PROJECT_BINARY_DIR}/grapher_resources.c" "grapher")

# Need a linear optimizer. Will look for both GLPK and CPLEX.
find_package(GLPK)
find_package(CPLEX)

# Check which optimization package is found
set(FORCE_GLPK false CACHE BOOL "If GLPK and CPLEX are found, by default we build against CPLEX. Setting this to true forces to build against GLPK")
if(CPLEX_FOUND)
    if(GLPK_FOUND AND FORCE_GLPK)
        set(OPTIMIZER_INCLUDE_DIRS ${GLPK_INCLUDE_DIR})
        set(OPTIMIZER_LIBRARIES ${GLPK_LIBRARY})
	set(OPTIMIZER "GLPK")
    else()
        set(OPTIMIZER_INCLUDE_DIRS ${CPLEX_INCLUDE_DIRS})
	if(APPLE)
	    set(OPTIMIZER_LIBRARIES -Wl,-force_load ${CPLEX_LIBRARIES})
	elseif(UNIX)
	    set(OPTIMIZER_LIBRARIES -Wl,-whole-archive ${CPLEX_LIBRARIES} -Wl,-no-whole-archive)
	endif()
	   set(OPTIMIZER "CPLEX")
    endif()
else()
    if(GLPK_FOUND)
        set(OPTIMIZER_INCLUDE_DIRS ${GLPK_INCLUDE_DIR})
        set(OPTIMIZER_LIBRARIES ${GLPK_LIBRARY})
	      set(OPTIMIZER "GLPK")
    else()
        set(OPTIMIZER "NONE")
        message(STATUS "No LP optimizer found at all!")
    endif()
endif()

configure_file(
  "${PROJECT_SOURCE_DIR}/src/lp/config.h.in"
  "${PROJECT_BINARY_DIR}/lp_config.h")
include_directories(${OPTIMIZER_INCLUDE_DIRS} ${PROJECT_BINARY_DIR})

# Common functionality
set(NCODE_COMMON_HEADER_FILES src/common.h src/substitute.h src/logging.h src/file.h src/stringpiece.h src/strutil.h src/map_util.h src/stl_util.h src/event_queue.h src/free_list.h src/packer.h src/ptr_queue.h src/lru_cache.h src/perfect_hash.h src/alphanum.h src/md5.h src/stats.h src/circular_array.h src/thread_runner.h src/status.h src/statusor.h)
add_library(ncode_common OBJECT src/common.cc src/substitute.cc src/logging.cc src/file.cc src/stringpiece.cc src/strutil.cc src/event_queue.cc src/packer.cc src/md5.cc src/stats.cc src/status.cc src/statusor.cc ${NCODE_COMMON_HEADER_FILES})

# Graph algorithms and pcap interface
set(NET_HEADER_FILES src/net/net_common.h src/net/net_gen.h src/net/pcap.h src/net/algorithm.h src/net/trie.h src/net/graph_query.h)
add_library(ncode_net OBJECT src/net/net_common.cc src/net/net_gen.cc src/net/pcap.cc src/net/algorithm.cc src/net/graph_query.cc ${NET_HEADER_FILES})

# Code "stolen" from Google's ctemplates
add_library(ctemplate OBJECT src/viz/ctemplate/template.cc
            src/viz/ctemplate/template_dictionary.cc
            src/viz/ctemplate/template_modifiers.cc
            src/viz/ctemplate/template_annotator.cc
            src/viz/ctemplate/template_pathops.cc
            src/viz/ctemplate/per_expand_data.cc
            src/viz/ctemplate/template_string.cc
            src/viz/ctemplate/template_cache.cc
            src/viz/ctemplate/htmlparser/htmlparser.cc
            src/viz/ctemplate/htmlparser/jsparser.cc
            src/viz/ctemplate/htmlparser/statemachine.cc
            src/viz/ctemplate/base/arena.cc)

set(CTEMPLATE_HEADER_FILES src/viz/ctemplate/template.h
            src/viz/ctemplate/template_dictionary.h
            src/viz/ctemplate/template_dictionary_interface.h
            src/viz/ctemplate/template_namelist.h
            src/viz/ctemplate/template_modifiers.h
            src/viz/ctemplate/template_annotator.h
            src/viz/ctemplate/template_pathops.h
            src/viz/ctemplate/per_expand_data.h
            src/viz/ctemplate/template_string.h
            src/viz/ctemplate/template_cache.h
            src/viz/ctemplate/template_emitter.h
            src/viz/ctemplate/template_enums.h
            src/viz/ctemplate/str_ref.h)

# Silence warnings about unused stuff in ctemplates
set_target_properties(ctemplate PROPERTIES COMPILE_FLAGS
                      "-Wno-unused-parameter -Wno-unused-const-variable -Wno-sign-compare -Wno-unused-private-field")

# Visualization and HTML
set(VIZ_HEADER_FILES src/viz/web_page.h src/viz/graph.h src/viz/grapher.h src/viz/server.h src/viz/square_grid.h)
add_library(ncode_viz OBJECT src/viz/web_page.cc src/viz/graph.cc src/viz/grapher.cc src/viz/server.cc src/viz/square_grid.cc ${PROJECT_BINARY_DIR}/www_resources.c ${PROJECT_BINARY_DIR}/grapher_resources.c ${VIZ_HEADER_FILES})

# LP optimization
set(LP_HEADER_FILES src/lp/lp.h src/lp/mc_flow.h src/lp/demand_matrix.h)
add_library(ncode_lp OBJECT src/lp/lp.cc src/lp/mc_flow.cc src/lp/demand_matrix.cc)

# HTSim
set(HTSIM_HEADER_FILES src/htsim/match.h src/htsim/packet.h src/htsim/htsim.h src/htsim/bulk_gen.h src/htsim/animator.h src/htsim/queue.h src/htsim/pcap_consumer.h src/htsim/tcp.h src/htsim/udp.h src/htsim/network.h src/htsim/flow_driver.h src/htsim/flow_counter.h src/htsim/physical.h)
add_library(ncode_htsim OBJECT src/htsim/match.cc src/htsim/packet.cc src/htsim/bulk_gen.cc src/htsim/animator.cc src/htsim/queue.cc src/htsim/pcap_consumer.cc src/htsim/tcp.cc src/htsim/udp.cc src/htsim/network.cc src/htsim/flow_driver.cc src/htsim/flow_counter.cc src/htsim/physical.cc)

add_library(ncode SHARED $<TARGET_OBJECTS:ncode_common> $<TARGET_OBJECTS:ncode_net> $<TARGET_OBJECTS:ctemplate> $<TARGET_OBJECTS:ncode_viz> $<TARGET_OBJECTS:ncode_lp> $<TARGET_OBJECTS:ncode_htsim>)
target_link_libraries(ncode ${PCAP_LIBRARY} ${OPTIMIZER_LIBRARIES})

install(TARGETS ncode
	LIBRARY DESTINATION lib 
	ARCHIVE DESTINATION lib/static)

install(FILES ${NCODE_COMMON_HEADER_FILES} DESTINATION include/ncode)
install(FILES ${NET_HEADER_FILES} DESTINATION include/ncode/net)
install(FILES ${VIZ_HEADER_FILES} DESTINATION include/ncode/viz)
install(FILES ${LP_HEADER_FILES} DESTINATION include/ncode/lp)
install(FILES ${HTSIM_HEADER_FILES} DESTINATION include/ncode/htsim)
install(FILES ${CTEMPLATE_HEADER_FILES} DESTINATION include/ncode/viz/ctemplate)

if (NOT NCODE_DISABLE_TESTS)
   set_property(SOURCE src/stringpiece_test.cc APPEND_STRING PROPERTY COMPILE_FLAGS "-Wno-conversion-null -Wno-sign-compare")
   set_property(SOURCE src/strutil_test.cc APPEND_STRING PROPERTY COMPILE_FLAGS "-Wno-sign-compare")
   add_test_exec(stringpiece_test src/stringpiece_test.cc ncode)
   add_test_exec(logging_test src/logging_test.cc ncode)
   add_test_exec(common_test src/common_test.cc ncode)
   add_test_exec(strutil_test src/strutil_test.cc ncode)
   add_test_exec(event_queue_test src/event_queue_test.cc ncode)
   add_test_exec(free_list_test src/free_list_test.cc ncode)
   add_test_exec(packer_test src/packer_test.cc ncode)
   add_test_exec(ptr_queue_test src/ptr_queue_test.cc ncode)
   add_test_exec(circular_array_test src/circular_array_test.cc ncode)
   add_test_exec(lru_cache_test src/lru_cache_test.cc ncode)
   add_test_exec(thread_runner_test src/thread_runner_test.cc ncode)
   add_test_exec(perfect_hash_test src/perfect_hash_test.cc ncode)
   add_test_exec(alphanum_test src/alphanum_test.cc ncode)
   add_test_exec(stats_test src/stats_test.cc ncode)
   add_test_exec(status_test src/status_test.cc ncode)
   add_test_exec(statusor_test src/statusor_test.cc ncode)

   add_test_exec(net_common_test src/net/net_common_test.cc ncode)
   add_test_exec(net_gen_test src/net/net_gen_test.cc ncode)
   add_test_exec(net_algorithm_test src/net/algorithm_test.cc ncode)
   add_test_exec(net_trie_test src/net/trie_test.cc ncode)
   add_test_exec(net_graph_query_test src/net/graph_query_test.cc ncode)

   add_test_exec(web_page_test src/viz/web_page_test.cc ncode)
   add_test_exec(square_grid_test src/viz/square_grid_test.cc ncode)
   add_test_exec(graph_test src/viz/graph_test.cc ncode)
   add_test_exec(grapher_test src/viz/grapher_test.cc ncode)
   add_test_exec(server_test src/viz/server_test.cc ncode)

   file(COPY data/pcap_test_data DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
   add_test_exec(htsim_match_test src/htsim/match_test.cc ncode)
   add_test_exec(htsim_packet_test src/htsim/packet_test.cc ncode)
   add_test_exec(htsim_bulk_gen_test src/htsim/bulk_gen_test.cc ncode)
   add_test_exec(htsim_animator_test src/htsim/animator_test.cc ncode)
   add_test_exec(htsim_pcap_consumer_test src/htsim/pcap_consumer_test.cc ncode)
   add_test_exec(htsim_network_test src/htsim/network_test.cc ncode)
   add_test_exec(htsim_flow_driver_test src/htsim/flow_driver_test.cc ncode)

   if (NOT ${OPTIMIZER} MATCHES "NONE")
     add_test_exec(lp_test src/lp/lp_test.cc ncode)
     add_test_exec(lp_mc_flow_test src/lp/mc_flow_test.cc ncode)
     add_test_exec(lp_demand_matrix_test src/lp/demand_matrix_test.cc ncode)
   endif()
endif()

if (NOT NCODE_DISABLE_BENCHMARKS)
   add_executable(perfect_hash_benchmark src/perfect_hash_benchmark.cc)
   target_link_libraries(perfect_hash_benchmark ncode)

   add_executable(algorithm_benchmark src/net/algorithm_benchmark.cc)
   target_link_libraries(algorithm_benchmark ncode)
   
   add_executable(free_list_benchmark src/free_list_benchmark.cc)
   target_link_libraries(free_list_benchmark ncode)
endif()
