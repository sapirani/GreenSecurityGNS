#!/bin/bash

# Defaults
datanodes=3
measurement_session_id=""

print_help() {
  cat <<EOF
Usage: $0 ACTION [datanodes] [measurement_session_id] [OPTIONS]

ACTION:
  start_measurement | stop_measurement

Positional arguments:
  datanodes                    Number of datanodes (default: 3)
  measurement_session_id       Optional session ID

Options:
  -d, --datanodes N            Number of datanodes
  -s, --measurement_session_id ID  Measurement session ID
  -h, --help                   Show this help
EOF
}

# --- Parse action ---
if [ -z "$1" ]; then
  echo "Error: Action required (start_measurement|stop_measurement)"
  print_help
  exit 1
fi

action="$1"
shift

# --- Positional: datanodes, session ID ---
if [ "$#" -gt 0 ] && [[ "$1" != -* ]]; then
  datanodes="$1"
  shift
fi

if [ "$#" -gt 0 ] && [[ "$1" != -* ]]; then
  measurement_session_id="$1"
  shift
fi

# --- Options parsing ---
while [ "$#" -gt 0 ]; do
  case "$1" in
    -d|--datanodes)
      datanodes="$2"; shift 2 ;;
    --datanodes=*)
      datanodes="${1#*=}"; shift ;;
    -s|--measurement_session_id)
      measurement_session_id="$2"; shift 2 ;;
    --measurement_session_id=*)
      measurement_session_id="${1#*=}"; shift ;;
    -h|--help)
      print_help; exit 0 ;;
    *)
      echo "Unknown option: $1"; print_help; exit 1 ;;
  esac
done

# --- Validate numeric args ---
if ! echo "$datanodes" | grep -Eq '^[0-9]+$'; then
  echo "Error: datanodes must be a positive integer"
  exit 1
fi

# --- Node list ---
base_nodes="resourcemanager-1:65432,namenode-1:65432,historyserver-1:65432"
all_nodes="$base_nodes"
for i in $(seq 1 "$datanodes"); do
  all_nodes="$all_nodes,datanode-$i:65432"
done

# --- Build Python trigger command ---
cmd=(
  PYTHONPATH=/green_security_measurements/Scanner
  /green_security_measurements/green_security_venv/bin/python
  -m scanner_trigger.trigger_sender
  "$action"
)

# Only include session ID if present
[ -n "$measurement_session_id" ] && cmd+=(--session_id "$measurement_session_id")

# Always include node addresses
cmd+=(-r "$all_nodes")

# --- Debug output ---
echo "Running scanner trigger command:"
printf '%s ' "${cmd[@]}"
echo

# --- Execute ---
"${cmd[@]}"
