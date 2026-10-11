#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
module_root="$(cd "$test_dir/../.." && pwd)"
libxr="$module_root/../Middlewares/Third_Party/LibXR"
includes=()
while IFS= read -r -d '' path; do includes+=(-isystem "$path"); done < <(find "$libxr/src" -type d -print0)
"${CXX:-c++}" -std=c++20 -DXR_LOG_MESSAGE_MAX_LEN=128 -DLIBXR_DEFAULT_SCALAR=float -Wall -Wextra -Werror \
  -isystem "$libxr/lib/Eigen" -isystem "$libxr/system/linux" "${includes[@]}" \
  "$test_dir/async_tx_gate_test.cpp" -o /tmp/shared_topic_tx_gate_test
/tmp/shared_topic_tx_gate_test
