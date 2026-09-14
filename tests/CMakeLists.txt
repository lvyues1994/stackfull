find_package(GTest CONFIG QUIET)
if(NOT GTest_FOUND)
    include(FetchContent)
    FetchContent_Declare(googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.14.0)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
endif()

include(GoogleTest)

function(stackfull_add_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PRIVATE
        stackfull::coro
        GTest::gtest GTest::gtest_main
        stackfull_warnings)
    gtest_discover_tests(${name} DISCOVERY_MODE PRE_TEST)
endfunction()

stackfull_add_test(stackfull_fcontext_test FcontextTest.cpp)
stackfull_add_test(stackfull_stack_test StackTest.cpp)
stackfull_add_test(stackfull_coro_test CoroutineTest.cpp)
