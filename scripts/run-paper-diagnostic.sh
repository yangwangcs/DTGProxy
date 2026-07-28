#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 --backend rocksdb|postgresql|neo4j --output-dir DIR [--warmup-seconds N] [--measurement-seconds N] [--repetitions N]" >&2
}

backend=
output_dir=
warmup_seconds=1
measurement_seconds=3
repetitions=3
while (($#)); do
  case "$1" in
    --backend|--output-dir|--warmup-seconds|--measurement-seconds|--repetitions)
      (($# >= 2)) || { usage; exit 2; }
      option=$1
      value=$2
      shift 2
      case "$option" in
        --backend) backend=$value ;;
        --output-dir) output_dir=$value ;;
        --warmup-seconds) warmup_seconds=$value ;;
        --measurement-seconds) measurement_seconds=$value ;;
        --repetitions) repetitions=$value ;;
      esac
      ;;
    *) usage; exit 2 ;;
  esac
done

[[ $backend =~ ^(rocksdb|postgresql|neo4j)$ ]] || { usage; exit 2; }
[[ -n $output_dir && $output_dir == /* ]] || { echo "--output-dir must be an absolute path" >&2; exit 2; }
[[ ! -e $output_dir ]] || { echo "output already exists: $output_dir" >&2; exit 2; }
for value in "$warmup_seconds" "$measurement_seconds" "$repetitions"; do
  [[ $value =~ ^[1-9][0-9]*$ ]] || { echo "diagnostic durations and repetitions must be positive integers" >&2; exit 2; }
done

export DTGPROXY_DIAGNOSTIC_BACKEND=$backend
export DTGPROXY_DIAGNOSTIC_OUTPUT_DIR=$output_dir
export DTGPROXY_DIAGNOSTIC_WARMUP_SECONDS=$warmup_seconds
export DTGPROXY_DIAGNOSTIC_MEASUREMENT_SECONDS=$measurement_seconds
export DTGPROXY_DIAGNOSTIC_REPETITIONS=$repetitions

cargo test -p paper-benchmark --test diagnostic_capture capture_selected_backend_diagnostic -- --exact --nocapture
