#!/bin/sh
set -eu

if [ "$#" -ne 4 ]; then
  echo "usage: $0 <package> <features> <test-target> <test-name>" >&2
  exit 2
fi

package=$1
features=$2
test_target=$3
test_name=$4

list=$(cargo test --locked -p "$package" --features "$features" --test "$test_target" -- --list)
count=$(printf '%s\n' "$list" | awk -F': ' -v name="$test_name" '$1 == name && $2 == "test" { count++ } END { print count + 0 }')
if [ "$count" -ne 1 ]; then
  echo "expected exactly one test named $test_name in $package/$test_target, found $count" >&2
  exit 1
fi

exec cargo test --locked -p "$package" --features "$features" --test "$test_target" \
  "$test_name" -- --exact --test-threads=1
