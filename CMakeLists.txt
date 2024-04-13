cmake_minimum_required(VERSION 3.12)
project(nstore)

set(CMAKE_CXX_STANDARD 17)
set(MKL_LINK_DIRECTORY "/opt/intel/mkl/lib/intel64")
set(MKL_INCLUDE_DIRECTORY "/opt/intel/mkl/include")

# MK for RMI
link_directories(${MKL_LINK_DIRECTORY})
include_directories(${MKL_INCLUDE_DIRECTORY})

set(CMAKE_CXX_FLAGS "-lnuma -lrt -lpthread -ldl -mrtm -msse4.1 -mavx2 -fopenmp -march=native -g")

add_definitions(-DNDEBUG)
add_definitions(-DPMEM)
add_definitions(-DPMDK)

include_directories(include)
include_directories(benchmark)
include_directories(include/util)
include_directories(src)
include_directories(third)

set(CLHT_SRC
    src/clht/clht_gc.cc
    src/clht/clht_lb_res.cc
    src/clht/clht_lf_res.cc
    src/clht/measurements.cc
    src/clht/ssmem.cc
)

add_library(clht SHARED ${CLHT_SRC})

add_compile_definitions(CORES_PER_SOCKET=16) # for clht

list(APPEND libs_to_link clht pmemobj pmem tbb papi tcmalloc_minimal pthread ipmctl numa jemalloc)

add_executable(example test/example.cc)
target_link_libraries(example PRIVATE ${libs_to_link})