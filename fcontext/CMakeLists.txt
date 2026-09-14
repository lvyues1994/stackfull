# L0 — boost.fcontext assembly primitives.
#
# One target per build, three architecture families. The OS never enters the
# selection: Linux, Android and QNX all use ELF + GNU as syntax with the same
# SysV / AAPCS calling conventions.

string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" stackfull_cpu)

if(stackfull_cpu MATCHES "^(x86_64|amd64)$")
    set(stackfull_fcontext_abi "x86_64_sysv")
elseif(stackfull_cpu MATCHES "^(aarch64|aarch64le|arm64)$")
    set(stackfull_fcontext_abi "arm64_aapcs")
elseif(stackfull_cpu MATCHES "^(arm|armv7|armv7-a|armv7l|armv7le|armhf|armel)$")
    set(stackfull_fcontext_abi "arm_aapcs")
else()
    message(FATAL_ERROR
        "stackfull: unsupported CMAKE_SYSTEM_PROCESSOR '${CMAKE_SYSTEM_PROCESSOR}'. "
        "Supported: x86_64, aarch64, armv7.")
endif()

set(stackfull_fcontext_asm
    asm/make_${stackfull_fcontext_abi}_elf_gas.S
    asm/jump_${stackfull_fcontext_abi}_elf_gas.S
    asm/ontop_${stackfull_fcontext_abi}_elf_gas.S)

add_library(stackfull_fcontext STATIC
    ${stackfull_fcontext_asm}
    include/stackfull/fcontext/Fcontext.h
    src/Fcontext.cpp)
add_library(stackfull::fcontext ALIAS stackfull_fcontext)

target_include_directories(stackfull_fcontext PUBLIC include)

# Rename the upstream symbols so a consumer may still link Boost.Context.
# `.S` sources pass through the C preprocessor, so plain -D works on labels,
# .globl / .hidden / .type / .size directives alike.
target_compile_definitions(stackfull_fcontext PRIVATE
    make_fcontext=stackfull_make_fcontext
    jump_fcontext=stackfull_jump_fcontext
    ontop_fcontext=stackfull_ontop_fcontext)

target_link_libraries(stackfull_fcontext
    PUBLIC stackfull::options
    PRIVATE stackfull_warnings)
