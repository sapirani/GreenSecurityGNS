#!/bin/sh

# Defaults
datanodes=3
measurement_session_id=""
mappers=""
reducers=""

print_help() {
  cat <<EOF
Usage: $0 ACTION [datanodes] [measurement_session_id] [OPTIONS]

ACTION:
  start_measurement | stop_measurement

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

# Validate first argument is action
if [ -z "$1" ]; then
  echo "Error: Action required (start_measurement|stop_measurement)"
  print_help
  exit 1
fi

action="$1"
shift

# Parse positional args first: datanodes, then measurement_session_id
if [ "$#" -gt 0 ] && [ "${1#-}" = "$1" ]; then
  datanodes="$1"
  shift
fi

if [ "$#" -gt 0 ] && [ "${1#-}" = "$1" ]; then
  measurement_session_id="$1"
  shift
fi

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

# Build node addresses
base_nodes_addresses="resourcemanager-1:65432,namenode-1:65432,historyserver-1:65432"
all_nodes_addresses="$base_nodes_addresses"
for i in $(seq 1 "$datanodes"); do
  all_nodes_addresses="$all_nodes_addresses,datanode-$i:65432"
done

# Build python command array
cmd=(
  PYTHONPATH=/green_security_measurements/Scanner
  /green_security_measurements/green_security_venv/bin/python
  -m scanner_trigger.trigger_sender
  "$action"
)

[ -n "$measurement_session_id" ] && cmd+=("$measurement_session_id")

cmd+=(
  -r "$all_nodes_addresses"
  --mappers "$mappers"
  --reducers "$reducers"
)

# Run
"${cmd[@]}"
