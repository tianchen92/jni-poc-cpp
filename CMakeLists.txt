cmake_minimum_required(VERSION 3.10)
project(testCmake)

find_package(JNI REQUIRED)
# 加入jni支持
include_directories(${JAVA_INCLUDE_PATH})
include_directories(${JAVA_INCLUDE_PATH2})

include_directories(/Users/tianchen/Workspace/arrow/cpp/src)
link_directories("/Users/tianchen/Workspace/arrow/cpp/release/release")
link_libraries(arrow)

set(CMAKE_CXX_STANDARD 11)


add_library(test_jni SHARED com_odps_arrow_TestArrowJni.cpp com_odps_arrow_TestArrowJni.h)
target_link_libraries(test_jni arrow)