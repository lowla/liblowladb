cmake_minimum_required(VERSION 2.6 FATAL_ERROR)
project(liblowladb)

enable_testing()

include_directories(../src ../src/datastore)

add_executable(tests ../test/lowladb_tests.cpp
                     ../test/main.cpp
                     ../platform-src/pi/ConvertUTF.cpp
                     ../platform-src/pi/utf16string.cpp
                     ../platform-src/pi/integration_pi.cpp
                     ../src/SqliteCursor.cpp
                     ../src/SqliteKey.cpp
                     ../src/TeamstudioException.cpp
                     ../src/lowladb.cpp
                     ../src/utf16stringbuilder.cpp
                     ../src/bson/bson.c
                     ../src/bson/encoding.c
                     ../src/bson/numbers.c
                     ../src/datastore/backup.c
                     ../src/datastore/bitvec.c
                     ../src/datastore/btmutex.c
                     ../src/datastore/btree.c
                     ../src/datastore/callback.c
                     ../src/datastore/fault.c
                     ../src/datastore/global.c
                     ../src/datastore/hash.c
                     ../src/datastore/malloc.c
                     ../src/datastore/mem1.c
                     ../src/datastore/memjournal.c
                     ../src/datastore/mutex.c
                     ../src/datastore/mutex_unix.c
                     ../src/datastore/os.c
                     ../src/datastore/os_unix.c
                     ../src/datastore/pager.c
                     ../src/datastore/pcache.c
                     ../src/datastore/pcache1.c
                     ../src/datastore/printf.c
                     ../src/datastore/random.c
                     ../src/datastore/sqmain.c
                     ../src/datastore/status.c
                     ../src/datastore/utf.c
                     ../src/datastore/util.c
                     ../src/datastore/vdbe.c
                     ../src/datastore/vdbemem.c
                     ../src/json/jsoncpp.cpp
                     ../test/gtest-all.cc
)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAG} -std=gnu99")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++0x")
add_definitions("-DMONGO_USE_LONG_LONG_INT")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -ldl -lpthread")

add_test(lowladb_tests tests)
