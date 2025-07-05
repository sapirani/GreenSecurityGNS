#!/bin/bash

# Defaults
mappers=""
reducers=""

print_help() {
  cat <<EOF
Usage: $0 [mappers] [reducers] [OPTIONS]

Positional arguments:
  mappers                 Number of mappers (default: 3)
  reducers                Number of reducers (default: 3)

Options:
  -m, --mappers N         Number of mappers
  -r, --reducers N        Number of reducers
  -h, --help              Show this help message
EOF
}

# Collect positional args
pos_args=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    -m|--mappers)
      mappers="$2"
      shift 2
      ;;
    --mappers=*)
      mappers="${1#*=}"
      shift
      ;;
    -r|--reducers)
      reducers="$2"
      shift 2
      ;;
    --reducers=*)
      reducers="${1#*=}"
      shift
      ;;
    -h|--help)
      print_help
      exit 0
      ;;
    -*)
      echo "Unknown option: $1"
      print_help
      exit 1
      ;;
    *)
      pos_args+=("$1")
      shift
      ;;
  esac
done

# Apply positional values
[ -z "$mappers" ]  && [ -n "${pos_args[0]}" ] && mappers="${pos_args[0]}"
[ -z "$reducers" ] && [ -n "${pos_args[1]}" ] && reducers="${pos_args[1]}"

# Set defaults if still unset
[ -z "$mappers" ] && mappers=3
[ -z "$reducers" ] && reducers=3

# Validate numeric input
for val in "$mappers" "$reducers"; do
  if ! echo "$val" | grep -Eq '^[0-9]+$'; then
    echo "Error: mappers and reducers must be positive integers"
    exit 1
  fi
done

echo "Running Hadoop job with:"
echo "  Mappers:  $mappers"
echo "  Reducers: $reducers"

# Run Hadoop job
hadoop jar /opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.maps="$mappers" \
  -D mapreduce.job.reduces="$reducers" \
  -input hdfs://namenode-1:9000/input \
  -output hdfs://namenode-1:9000/output \
  -mapper /home/mapper.py \
  -reducer /home/reducer.py \
  -file /home/mapper.py \
  -file /home/reducer.py
