cmake_minimum_required(VERSION 3.1.0)
project(
    dctx
    LANGUAGES C
    VERSION 0.1.0
)

if("${LIBUV_INCLUDE}" STREQUAL "")
    find_path(LIBUV_INCLUDE NAMES uv.h)
endif()

if("${LIBUV_LIB}" STREQUAL "")
    find_library(LIBUV_LIB NAMES uv)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

function(default_compile_options tgt)
    target_compile_options("${tgt}" PRIVATE "-Werror")
    target_compile_options("${tgt}" PRIVATE "-Wall")
    target_compile_options("${tgt}" PRIVATE "-Wextra")
    target_compile_options("${tgt}" PRIVATE "-Wswitch-enum")
    target_compile_options("${tgt}" PRIVATE "-Wstrict-overflow")
    target_compile_options("${tgt}" PRIVATE "-Wconversion")
    target_compile_options("${tgt}" PRIVATE "-Wstrict-prototypes")
    target_compile_options("${tgt}" PRIVATE "-Wmissing-prototypes")
    # target_compile_options("${tgt}" PRIVATE "-fstrict-aliasing")
    # target_compile_options("${tgt}" PRIVATE "-Wstrict-aliasing=2")
    target_compile_options("${tgt}" PRIVATE "-Wno-missing-field-initializers")
    target_compile_options("${tgt}" PRIVATE "-Wvla")
    target_compile_options("${tgt}" PRIVATE "-fdiagnostics-color=always")
    # address sanitizer
    # target_compile_options("${tgt}" PRIVATE "-fsanitize=address" "-fno-omit-frame-pointer")
    # target_link_libraries("${tgt}" PRIVATE "-fsanitize=address" "-fno-omit-frame-pointer")

    # executables only:
    get_target_property(tgt_type "${tgt}" TYPE)
    if("${tgt_type}" STREQUAL "EXECUTABLE")
        target_link_libraries("${tgt}" PRIVATE "-pie")
    endif()
endfunction()

function(source_compile_options src)
    get_property(opts SOURCE "${src}" PROPERTY COMPILE_OPTIONS)
    list(APPEND opts ${ARGN})
    set_property(SOURCE "${src}" PROPERTY COMPILE_OPTIONS ${opts})
endfunction()

# build a library out of dctx.c
add_library(dctx SHARED dctx.c op.c server.c client.c const.c link.c)
target_link_libraries(dctx PUBLIC pthread "${LIBUV_LIB}")
default_compile_options(dctx)

source_compile_options(const.c  "-Wno-discarded-qualifiers")

# build a test around libdctx
add_executable(test test.c)
target_link_libraries(test PUBLIC dctx)
default_compile_options(test)
