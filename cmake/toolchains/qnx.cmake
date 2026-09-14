# QNX Neutrino toolchain (qcc / q++), driven by two inputs:
#
#   STACKFULL_QNX_ARCH     aarch64le (default) | x86_64
#   QNX_HOST / QNX_TARGET  from `source <sdp>/qnxsdp-env.sh`
#
# Usage: cmake --preset qnx-aarch64le   (see CMakePresets.json)

if(NOT DEFINED ENV{QNX_HOST} OR NOT DEFINED ENV{QNX_TARGET})
    message(FATAL_ERROR "QNX_HOST / QNX_TARGET are not set; source qnxsdp-env.sh first")
endif()

set(STACKFULL_QNX_ARCH "aarch64le" CACHE STRING "QNX target architecture: aarch64le | x86_64")

if(STACKFULL_QNX_ARCH STREQUAL "aarch64le")
    set(CMAKE_SYSTEM_PROCESSOR aarch64)
    set(stackfull_qnx_target gcc_ntoaarch64le)
elseif(STACKFULL_QNX_ARCH STREQUAL "x86_64")
    set(CMAKE_SYSTEM_PROCESSOR x86_64)
    set(stackfull_qnx_target gcc_ntox86_64)
else()
    message(FATAL_ERROR "Unsupported STACKFULL_QNX_ARCH '${STACKFULL_QNX_ARCH}'")
endif()

set(CMAKE_SYSTEM_NAME QNX)
set(CMAKE_SYSROOT "$ENV{QNX_TARGET}")

set(CMAKE_C_COMPILER   "$ENV{QNX_HOST}/usr/bin/qcc")
set(CMAKE_CXX_COMPILER "$ENV{QNX_HOST}/usr/bin/q++")
set(CMAKE_ASM_COMPILER "$ENV{QNX_HOST}/usr/bin/qcc")
set(CMAKE_C_COMPILER_TARGET   ${stackfull_qnx_target})
set(CMAKE_CXX_COMPILER_TARGET ${stackfull_qnx_target})
set(CMAKE_ASM_COMPILER_TARGET ${stackfull_qnx_target})

set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)
