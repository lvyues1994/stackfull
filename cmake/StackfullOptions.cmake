# Two INTERFACE targets carry the project's shared usage requirements.
#
#   stackfull::options   PUBLIC  — definitions that change header layout / API
#                                  semantics; every consumer must see the same
#                                  values, so they travel with the library.
#   stackfull_warnings   PRIVATE — warning set and sanitizer flags for code that
#                                  belongs to this repository only.

add_library(stackfull_options INTERFACE)
add_library(stackfull::options ALIAS stackfull_options)

if(STACKFULL_EXCEPTIONS)
    target_compile_definitions(stackfull_options INTERFACE STACKFULL_HAS_EXCEPTIONS=1)
else()
    target_compile_definitions(stackfull_options INTERFACE STACKFULL_HAS_EXCEPTIONS=0)
endif()

if(STACKFULL_SWAP_EH_GLOBALS AND STACKFULL_EXCEPTIONS)
    target_compile_definitions(stackfull_options INTERFACE STACKFULL_SWAP_EH_GLOBALS=1)
else()
    target_compile_definitions(stackfull_options INTERFACE STACKFULL_SWAP_EH_GLOBALS=0)
endif()

# ---------------------------------------------------------------------------
add_library(stackfull_warnings INTERFACE)

target_compile_options(stackfull_warnings INTERFACE
    $<$<COMPILE_LANGUAGE:CXX>:
        -Wall -Wextra -Wpedantic
        -Wold-style-cast
        -Wzero-as-null-pointer-constant
        -Wnon-virtual-dtor
        -Wconversion -Wsign-compare
        -Wcast-align
        -Werror=return-type
        -Werror=uninitialized
        -Werror=vla
        -Werror=suggest-override
        -Werror=delete-non-virtual-dtor
    >)

if(NOT STACKFULL_EXCEPTIONS)
    target_compile_options(stackfull_warnings INTERFACE
        $<$<COMPILE_LANGUAGE:CXX>:-fno-exceptions>)
endif()

# ---------------------------------------------------------------------------
# Developer sanitizer profile — uniform across the whole build tree.
# ---------------------------------------------------------------------------
if(STACKFULL_SANITIZE)
    add_compile_options(-fsanitize=${STACKFULL_SANITIZE} -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=${STACKFULL_SANITIZE})
endif()
