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
find_package(Threads REQUIRED)

# stackfull_add_test(<name> LABEL <label> LIBS <targets...> SOURCES <files...>)
#
# Labels let a preset run a subset: the `tsan` preset only runs `queue`,
# because ThreadSanitizer needs fiber annotations the coroutine layer does
# not carry yet.
function(stackfull_add_test name)
    cmake_parse_arguments(arg "" "LABEL" "LIBS;SOURCES" ${ARGN})
    add_executable(${name} ${arg_SOURCES})
    target_link_libraries(${name} PRIVATE ${arg_LIBS} GTest::gtest GTest::gtest_main stackfull_warnings)
    if(STACKFULL_SANITIZE STREQUAL "thread" AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
        # ThreadSanitizer's shadow layout rejects the 32-bit ASLR entropy of
        # recent kernels ("unexpected memory mapping"); run tests without ASLR.
        set_property(TARGET ${name} PROPERTY TEST_LAUNCHER setarch ${CMAKE_HOST_SYSTEM_PROCESSOR} -R)
    endif()
    # A task body that returns early (a failed ASSERT) can leave the test
    # waiting forever; the timeout turns such a hang into a failure.
    gtest_discover_tests(${name} DISCOVERY_MODE PRE_TEST PROPERTIES LABELS ${arg_LABEL} TIMEOUT 60)
endfunction()

stackfull_add_test(stackfull_fcontext_test LABEL coro LIBS stackfull::coro SOURCES FcontextTest.cpp)
stackfull_add_test(stackfull_stack_test    LABEL coro LIBS stackfull::coro SOURCES StackTest.cpp)
stackfull_add_test(stackfull_coro_test     LABEL coro LIBS stackfull::coro SOURCES CoroutineTest.cpp)
stackfull_add_test(stackfull_queue_test    LABEL queue LIBS stackfull::queue Threads::Threads SOURCES QueueTest.cpp)
stackfull_add_test(stackfull_sched_test    LABEL sched LIBS stackfull::sched SOURCES SchedulerTest.cpp)
stackfull_add_test(stackfull_sync_test     LABEL sync LIBS stackfull::sync SOURCES SyncTest.cpp)
stackfull_add_test(stackfull_timer_test    LABEL sched LIBS stackfull::sync SOURCES TimerTest.cpp)
stackfull_add_test(stackfull_io_test       LABEL io LIBS stackfull::io stackfull::sync SOURCES IoTest.cpp)
stackfull_add_test(stackfull_bridge_test   LABEL sync LIBS stackfull::runtime SOURCES BridgeTest.cpp)
