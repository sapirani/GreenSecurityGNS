#!/bin/bash

# Defaults
datanodes=""
measurement_session_id=""
mappers=""
reducers=""

print_help() {
  cat <<EOF
Usage: $0 [datanodes] [measurement_session_id] [mappers] [reducers] [OPTIONS]

Positional arguments:
  datanodes                    Number of datanodes (default: 3)
  measurement_session_id       Optional session ID
  mappers                      Number of mappers (default: datanodes)
  reducers                     Number of reducers (default: datanodes)

Options:
  -d, --datanodes N            Number of datanodes
  -s, --measurement_session_id ID  Measurement session ID
  -m, --mappers N              Number of mappers
  -r, --reducers N             Number of reducers
  -h, --help                   Show this help message
EOF
}

# Parse positional arguments first
pos_args=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    -d|--datanodes|--datanodes=*|-s|--measurement_session_id|--measurement_session_id=*|-m|--mappers|--mappers=*|-r|--reducers|--reducers=*|-h|--help)
      break
      ;;
    *)
      pos_args+=("$1")
      shift
      ;;
  esac
done

# Assign positional args
[ -n "${pos_args[0]}" ] && datanodes="${pos_args[0]}"
[ -n "${pos_args[1]}" ] && measurement_session_id="${pos_args[1]}"
[ -n "${pos_args[2]}" ] && mappers="${pos_args[2]}"
[ -n "${pos_args[3]}" ] && reducers="${pos_args[3]}"

# Parse options
while [ "$#" -gt 0 ]; do
  case "$1" in
    -d|--datanodes)
      datanodes="$2"
      shift 2
      ;;
    --datanodes=*)
      datanodes="${1#*=}"
      shift
      ;;
    -s|--measurement_session_id)
      measurement_session_id="$2"
      shift 2
      ;;
    --measurement_session_id=*)
      measurement_session_id="${1#*=}"
      shift
      ;;
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
    *)
      echo "Unknown option: $1"
      print_help
      exit 1
      ;;
  esac
done

# Set defaults
[ -z "$datanodes" ] && datanodes=3
[ -z "$mappers" ] && mappers="$datanodes"
[ -z "$reducers" ] && reducers="$datanodes"

# Validate
for val in "$datanodes" "$mappers" "$reducers"; do
  if ! [[ "$val" =~ ^[0-9]+$ ]]; then
    echo "Error: datanodes, mappers, and reducers must be positive integers"
    exit 1
  fi
done

echo "Selected number of datanodes:" $datanodes
echo "Selected number of mappers:" $mappers
echo "Selected number of reducers:" $reducers

# === Trigger START measurement ===
/home/send_trigger.sh start_measurement "$datanodes" "$measurement_session_id"

# === Run Hadoop Streaming ===
hadoop jar /opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.maps="$mappers" \
  -D mapreduce.job.reduces="$reducers" \
  -input hdfs://namenode-1:9000/input \
  -output hdfs://namenode-1:9000/output \
  -mapper /home/mapper.py \
  -reducer /home/reducer.py \
  -file /home/mapper.py \
  -file /home/reducer.py

# === Trigger STOP measurement ===
/home/send_trigger.sh stop_measurement "$datanodes" "$measurement_session_id"
