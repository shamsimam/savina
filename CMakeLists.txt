cmake_minimum_required(VERSION 2.8)
project (savina_caf C CXX)

# cmake paths
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_CURRENT_BINARY_DIR}/bin")

# set build default build type to RelWithDebInfo if not set
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

# compiler setup
if(NOT CMAKE_CXX_FLAGS)
  set(CMAKE_CXX_FLAGS                   "-std=c++11 -Wextra -Wall -pedantic")
endif()
if(NOT CMAKE_CXX_FLAGS_DEBUG)
  set(CMAKE_CXX_FLAGS_DEBUG             "-O0 -g")
endif()
if(NOT CMAKE_CXX_FLAGS_MINSIZEREL)
  set(CMAKE_CXX_FLAGS_MINSIZEREL        "-Os")
endif()
if(NOT CMAKE_CXX_FLAGS_RELEASE)
  set(CMAKE_CXX_FLAGS_RELEASE           "-O3 -DNDEBUG")
endif()
if(NOT CMAKE_CXX_FLAGS_RELWITHDEBINFO)
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO    "-O2 -g -fno-omit-frame-pointer")
endif()

# load CAF library
# set(CAF_ROOT_DIR "path to caf build directory")
find_package(CAF COMPONENTS core REQUIRED)
set(LD_FLAGS ${LD_FLAGS} ${CAF_LIBRARIES})
include_directories(${CAF_INCLUDE_DIRS})

# add benchmark and helper classes
set(INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/main/cpp/edu/rice/habanero/")
include_directories(${INCLUDE_DIR})

message(STATUS ${INCLUDE_DIR})

# add targets
add_custom_target(all_benchmarks ALL)
macro(add_benchmark name path)
  if("${ARGN}" STREQUAL "")
    set(new_name ${name})
  else()
    string(REPLACE ";" "_" post_fix "${ARGN}")
    string(TOLOWER ${post_fix} post_fix)
    set(new_name "${name}_${post_fix}")
  endif()
  add_executable(${new_name} ${path} "${INCLUDE_DIR}/benchmark.cpp")
  target_link_libraries(${new_name} ${CAF_LIBRARIES} ${LD_FLAGS})
  target_compile_definitions(${new_name} PRIVATE ${ARGN})
  add_dependencies(all_benchmarks ${new_name})
endmacro()

macro(add_caf_benchmark name)
  add_benchmark(${name} ${CMAKE_CURRENT_SOURCE_DIR}/src/main/cpp/edu/rice/habanero/benchmarks/caf/${name}.cpp ${ARGN})
endmacro()

add_caf_benchmark(caf_01_pingpong)
add_caf_benchmark(caf_02_count)
add_caf_benchmark(caf_03_fjthrput)
add_caf_benchmark(caf_04_fjcreate)
add_caf_benchmark(caf_05_threadring)
add_caf_benchmark(caf_06_chameneos)
add_caf_benchmark(caf_07_big)
add_caf_benchmark(caf_08_concdict)
add_caf_benchmark(caf_09_concsll)
add_caf_benchmark(caf_10_bndbuffer)
add_caf_benchmark(caf_11_philosopher)
#add_caf_benchmark(caf_12_barber) bench uses busy waiting
#add_caf_benchmark(caf_13_cigsmok) bench uses busy waiting
add_caf_benchmark(caf_14_logmap REQUEST_AWAIT INFINITE)
add_caf_benchmark(caf_14_logmap REQUEST_AWAIT HIGH_TIMEOUT)
add_caf_benchmark(caf_14_logmap BECOME_UNBECOME_SLOW)
add_caf_benchmark(caf_14_logmap BECOME_UNBECOME_FAST)
add_caf_benchmark(caf_15_banking REQUEST_AWAIT INFINITE)
add_caf_benchmark(caf_15_banking REQUEST_AWAIT HIGH_TIMEOUT)
add_caf_benchmark(caf_15_banking REQUEST_THEN INFINITE)
add_caf_benchmark(caf_15_banking REQUEST_THEN HIGH_TIMEOUT)
add_caf_benchmark(caf_15_banking BECOME_UNBECOME_SLOW)
add_caf_benchmark(caf_15_banking BECOME_UNBECOME_FAST)
add_caf_benchmark(caf_16_radixsort)
#add_caf_benchmark(caf_17_filterbank) savina implementation is buggy 
add_caf_benchmark(caf_18_sieve)
#add_caf_benchmark(caf_19_uct) bench uses busy waiting
add_caf_benchmark(caf_20_facloc)
add_caf_benchmark(caf_21_trapezoid)
#add_caf_benchmark(caf_22_piprecision) bench uses BigDecimal
add_caf_benchmark(caf_23_recmatmul)
add_caf_benchmark(caf_24_quicksort)
add_caf_benchmark(caf_25_apsp)
#add_caf_benchmark(caf_26_sor) savina implementation is buggy
#add_caf_benchmark(caf_27_astar) bench uses busy waiting
add_caf_benchmark(caf_28_nqueenk)
add_caf_benchmark(caf_29_fib)
add_caf_benchmark(caf_30_bitonicsort)

string(TOUPPER "${CMAKE_BUILD_TYPE}" UPPER_BUILD_TYPE)
set(ALL_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPER_BUILD_TYPE}}")
# summary
if(NOT CAF_NO_SUMMARY)
  message(STATUS
        "\n====================|  Build Summary  |===================="
        "\n"
        "\nBuild type:        ${CMAKE_BUILD_TYPE}"
        "\n"
        "\nCXX:               ${CMAKE_CXX_COMPILER}"
        "\nCXXFLAGS:          ${ALL_CXX_FLAGS}"
        "\nLIBRARIES:         ${LD_FLAGS}"
        "\n"
        "\nSource directory:  ${CMAKE_CURRENT_SOURCE_DIR}"
        "\nBuild directory:   ${CMAKE_CURRENT_BINARY_DIR}"
        "\nExecutable path:   ${EXECUTABLE_OUTPUT_PATH}"
        "\n"
        "\n===========================================================\n")
endif()

