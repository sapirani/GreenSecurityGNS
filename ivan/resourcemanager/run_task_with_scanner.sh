#!/bin/sh

# Defaults
datanodes=3
measurement_session_id=""
mappers=""
reducers=""

print_help() {
  cat <<EOF
Usage: $0 [datanodes] [measurement_session_id] [OPTIONS]

Positional:
  datanodes              Number of datanodes (default: 3)
  measurement_session_id Optional session ID

Options:
  -d, --datanodes N         Number of datanodes
  -s, --measurement_session_id ID  Measurement session ID
  -p, --mappers N           Number of mappers
  -r, --reducers N          Number of reducers
  -h, --help                Show this help
EOF
}

# Parse positional args first: datanodes, measurement_session_id
positional_args=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    -d|--datanodes|--datanodes=*|-s|--measurement_session_id|--measurement_session_id=*|-p|--mappers|--mappers=*|-r|--reducers|--reducers=*|-h|--help)
      break
      ;;
    *)
      positional_args+=("$1")
      shift
      ;;
  esac
done

[ -n "${positional_args[0]}" ] && datanodes="${positional_args[0]}"
[ -n "${positional_args[1]}" ] && measurement_session_id="${positional_args[1]}"

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
    -p|--mappers)
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

# Validate numbers
for val in "$datanodes" "$mappers" "$reducers"; do
  if [ -n "$val" ] && ! echo "$val" | grep -Eq '^[0-9]+$'; then
    echo "Error: datanodes, mappers, and reducers must be positive integers"
    exit 1
  fi
done

# Defaults for mappers and reducers
[ -z "$mappers" ] && mappers="$datanodes"
[ -z "$reducers" ] && reducers="$datanodes"

# Start measurement
/home/send_trigger.sh start_measurement "$datanodes" "$measurement_session_id" \
  --mappers="$mappers" --reducers="$reducers"

# Run Hadoop streaming job
hadoop jar /opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.maps="$mappers" \
  -D mapreduce.job.reduces="$reducers" \
  -input hdfs://namenode-1:9000/input \
  -output hdfs://namenode-1:9000/output \
  -mapper /home/mapper.py \
  -reducer /home/reducer.py \
  -file /home/mapper.py \
  -file /home/reducer.py

# Stop measurement
/home/send_trigger.sh stop_measurement "$datanodes" "$measurement_session_id" \
  --mappers="$mappers" --reducers="$reducers"
