cmake_minimum_required(VERSION 3.16)

project(universal_thread_pool VERSION 0.1.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
find_package(Threads REQUIRED)

set(UNIVERSAL_THREAD_POOL_SANITIZER "" CACHE STRING "Enable sanitizer: address or thread")
set_property(CACHE UNIVERSAL_THREAD_POOL_SANITIZER PROPERTY STRINGS "" address thread)

add_library(universal_thread_pool
    src/c_api.cpp
    src/common.cpp
    src/executor.cpp
    src/runtime.cpp
    src/task_graph.cpp
    src/task_group.cpp
    src/thread_pool.cpp
)
add_library(universal_thread_pool::universal_thread_pool ALIAS universal_thread_pool)

target_include_directories(universal_thread_pool PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_compile_features(universal_thread_pool PUBLIC cxx_std_17)
target_link_libraries(universal_thread_pool PUBLIC Threads::Threads)
target_compile_definitions(universal_thread_pool PUBLIC
    UNIVERSAL_THREAD_POOL_VERSION_MAJOR=${PROJECT_VERSION_MAJOR}
    UNIVERSAL_THREAD_POOL_VERSION_MINOR=${PROJECT_VERSION_MINOR}
    UNIVERSAL_THREAD_POOL_VERSION_PATCH=${PROJECT_VERSION_PATCH}
    UNIVERSAL_THREAD_POOL_VERSION_STRING="${PROJECT_VERSION}"
)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(universal_thread_pool PRIVATE -Wall -Wextra -Wpedantic)
endif()

if (UNIVERSAL_THREAD_POOL_SANITIZER)
    if (NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        message(FATAL_ERROR "UNIVERSAL_THREAD_POOL_SANITIZER requires GCC or Clang")
    endif()

    if (UNIVERSAL_THREAD_POOL_SANITIZER STREQUAL "thread")
        target_compile_options(universal_thread_pool PUBLIC
            -fsanitize=thread
            -fno-omit-frame-pointer
            -g
        )
        target_link_options(universal_thread_pool PUBLIC -fsanitize=thread)
    elseif (UNIVERSAL_THREAD_POOL_SANITIZER STREQUAL "address")
        target_compile_options(universal_thread_pool PUBLIC
            -fsanitize=address,undefined
            -fno-omit-frame-pointer
            -g
        )
        target_link_options(universal_thread_pool PUBLIC -fsanitize=address,undefined)
    else()
        message(FATAL_ERROR
            "UNIVERSAL_THREAD_POOL_SANITIZER must be empty, address, or thread")
    endif()
endif()

install(TARGETS universal_thread_pool
    EXPORT universal_thread_poolTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/universal_thread_poolConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/universal_thread_poolConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/universal_thread_pool
)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/universal_thread_poolConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/universal_thread_poolConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/universal_thread_poolConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/universal_thread_pool
)

install(EXPORT universal_thread_poolTargets
    NAMESPACE universal_thread_pool::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/universal_thread_pool
)

option(UNIVERSAL_THREAD_POOL_BUILD_TESTS "Build universal_thread_pool tests and examples" ON)
option(UNIVERSAL_THREAD_POOL_BUILD_BENCHMARKS "Build universal_thread_pool benchmark harness" OFF)

if (UNIVERSAL_THREAD_POOL_BUILD_TESTS)
enable_testing()

add_executable(thread_pool_tests tests/thread_pool_tests.cpp)
target_link_libraries(thread_pool_tests PRIVATE universal_thread_pool)
add_test(NAME thread_pool_tests COMMAND thread_pool_tests)

add_executable(thread_pool_basic examples/basic.cpp)
target_link_libraries(thread_pool_basic PRIVATE universal_thread_pool)
endif()

if (UNIVERSAL_THREAD_POOL_BUILD_BENCHMARKS)
add_executable(thread_pool_bench benchmarks/thread_pool_bench.cpp)
target_link_libraries(thread_pool_bench PRIVATE universal_thread_pool)
endif()
