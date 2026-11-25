#!/bin/bash
#
# Runs every Rust example that finishes on its own, and fails if any exits
# with an error or does not finish within the timeout.
#
#   ./run_examples.sh                 # all of them
#   ./run_examples.sh kv_server ...   # only these
#
# EXAMPLE_TIMEOUT sets the per-example limit in seconds (default 90).

set -uo pipefail

timeout_secs="${EXAMPLE_TIMEOUT:-90}"
target_dir="${CARGO_TARGET_DIR:-target}"

# Interactive, or needs other processes running first.
skip=(
    cluster_control  # needs remote supervisors to connect to
    interactive_demo # reads commands from the terminal
    tmux_demo        # drives a tmux session
    tree_visualizer  # redraws the terminal until interrupted
)

if [[ $# -gt 0 ]]; then
    examples=("$@")
else
    mapfile -t examples < <(
        awk -F'"' '/^\[\[example\]\]/ { in_example = 1; next }
                   in_example && /^name/ { print $2; in_example = 0 }' Cargo.toml
    )
fi

cargo build --quiet --examples || exit 1

failed=()
for example in "${examples[@]}"; do
    if [[ " ${skip[*]} " == *" ${example} "* ]]; then
        printf '%-24s skipped\n' "$example"
        continue
    fi

    log="$(mktemp)"
    start=$SECONDS
    timeout "$timeout_secs" "$target_dir/debug/examples/$example" </dev/null >"$log" 2>&1
    status=$?
    elapsed=$((SECONDS - start))

    if [[ $status -eq 0 ]]; then
        printf '%-24s ok (%ss)\n' "$example" "$elapsed"
    else
        if [[ $status -eq 124 ]]; then
            printf '%-24s TIMED OUT after %ss\n' "$example" "$timeout_secs"
        else
            printf '%-24s FAILED with exit status %s\n' "$example" "$status"
        fi
        echo "---- last lines of output ----"
        tail -n 20 "$log"
        echo "------------------------------"
        failed+=("$example")
    fi
    rm -f "$log"
done

if [[ ${#failed[@]} -gt 0 ]]; then
    echo
    echo "failed: ${failed[*]}"
    exit 1
fi
