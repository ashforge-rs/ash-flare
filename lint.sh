#!/bin/bash

cargo clippy \
    "$@" \
    -D clippy::unwrap_used \
    -D clippy::expect_used \
    -D clippy::panic \
    -D clippy::exit \
    -D clippy::todo \
    -D clippy::unimplemented \
    -D clippy::unreachable \
    -D clippy::infinite_loop \
    -D clippy::cognitive_complexity \
    -D clippy::too_many_lines \
    -D clippy::too_many_arguments \
    -D clippy::fn_params_excessive_bools \
    -D clippy::wildcard_imports \
    -D clippy::missing_safety_doc \
    -D clippy::undocumented_unsafe_blocks \
    -D clippy::multiple_unsafe_ops_per_block \
    -D clippy::mem_forget \
    -D clippy::panic_in_result_fn \
    -W clippy::all \
   -W clippy::pedantic \
    -A clippy::must_use_candidate \
    -A clippy::return_self_not_must_use \
    -A clippy::missing_errors_doc \
    -A clippy::missing_panics_doc \
    -A clippy::doc_markdown \
    -A clippy::missing_fields_in_debug \
    -A clippy::unnecessary_wraps \
    -A clippy::unused_async \
    -A clippy::uninlined_format_args \
    -A clippy::manual_let_else \
    -A clippy::single_match_else \
    -A clippy::map_unwrap_or \
    -A clippy::cast_possible_truncation \
    -A clippy::unchecked_time_subtraction
