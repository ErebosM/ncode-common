# This module finds cplex.
#
# User can give CPLEX_ROOT_DIR as a hint stored in the cmake cache.
#
# It sets the following variables:
#  CPLEX_FOUND              - Set to false, or undefined, if cplex isn't found.
#  CPLEX_INCLUDE_DIRS       - include directory
#  CPLEX_LIBRARIES          - library files

set(CPLEX_ROOT_DIR "" CACHE PATH "CPLEX root directory.")

if(APPLE)
 file(GLOB CPLEX_INCLUDE_SEARCH_PATH "$ENV{HOME}/Applications/IBM/ILOG/*/cplex/include")
 file(GLOB CPLEX_LIB_SEARCH_PATH "$ENV{HOME}/Applications/IBM/ILOG/*/cplex/lib/x86-64_osx/static_pic")
elseif(UNIX)
 file(GLOB CPLEX_INCLUDE_SEARCH_PATH "/opt/ibm/ILOG/*/cplex/include" "$ENV{HOME}/ibm/ILOG/*/cplex/include")
 file(GLOB CPLEX_LIB_SEARCH_PATH "/opt/ibm/ILOG/*/cplex/lib/x86-64_linux/static_pic" "$ENV{HOME}/ibm/ILOG/*/cplex/lib/x86-64_linux/static_pic")
endif()

FIND_PATH(CPLEX_INCLUDE_DIR
  ilcplex/cplex.h
  HINTS ${CPLEX_ROOT_DIR}/cplex/include
        ${CPLEX_ROOT_DIR}/include
  PATHS ENV C_INCLUDE_PATH
        ENV C_PLUS_INCLUDE_PATH
        ENV INCLUDE_PATH
	${CPLEX_INCLUDE_SEARCH_PATH}
  )
message(STATUS "CPLEX Include dir: ${CPLEX_INCLUDE_DIR}")

FIND_LIBRARY(CPLEX_LIBRARY
  NAMES cplex
  HINTS ${CPLEX_LIB_SEARCH_PATH} 
  PATHS ENV LIBRARY_PATH #unix
        ENV LD_LIBRARY_PATH #unix
  )
message(STATUS "CPLEX Library: ${CPLEX_LIBRARY}")

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(CPLEX DEFAULT_MSG 
 CPLEX_LIBRARY CPLEX_INCLUDE_DIR)

IF(CPLEX_FOUND)
  SET(CPLEX_INCLUDE_DIRS ${CPLEX_INCLUDE_DIR})
  SET(CPLEX_LIBRARIES ${CPLEX_LIBRARY} )
  IF(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    SET(CPLEX_LIBRARIES "${CPLEX_LIBRARIES};m;pthread")
  ENDIF(CMAKE_SYSTEM_NAME STREQUAL "Linux")
ENDIF(CPLEX_FOUND)

MARK_AS_ADVANCED(CPLEX_LIBRARY CPLEX_INCLUDE_DIR)
