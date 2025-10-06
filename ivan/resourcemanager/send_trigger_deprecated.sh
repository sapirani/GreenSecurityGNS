#!/bin/bash

# Defaults
datanodes=3
measurement_session_id=""

print_help() {
  cat <<EOF
Usage: $0 ACTION [datanodes] [measurement_session_id] [OPTIONS]
ACTION:
  start_measurement | stop_measurement | stop_program

Positional arguments:
  datanodes                    Number of datanodes (default: 3)
  measurement_session_id       Optional session ID

Options:
  -d, --datanodes N                Number of datanodes
  -i, --measurement_session_id ID Measurement session ID
  -h, --help                       Show this help
EOF
}

# --- Early help check ---
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  print_help
  exit 0
fi

# --- Parse and validate action ---
if [ -z "$1" ]; then
  echo "Error: ACTION is required (start_measurement | stop_measurement | stop_program)"
  print_help
  exit 1
fi

action="$1"
shift

if [[ "$action" != "start_measurement" && "$action" != "stop_measurement" && "$action" != "stop_program" ]]; then
  echo "Error: Invalid ACTION '$action'"
  print_help
  exit 1
fi

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
    -i|--measurement_session_id)
      measurement_session_id="$2"; shift 2 ;;
    --measurement_session_id=*)
      measurement_session_id="${1#*=}"; shift ;;
    -h|--help)
      print_help; exit 0 ;;
    *)
      echo "Unknown option: $1"
      print_help
      exit 1 ;;
  esac
done

# --- Validate numeric datanodes ---
if ! echo "$datanodes" | grep -Eq '^[0-9]+$'; then
  echo "Error: datanodes must be a positive integer"
  exit 1
fi

# --- Build node list ---
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

# Optional session ID
[ -n "$measurement_session_id" ] && cmd+=(--session_id "$measurement_session_id")

# Required: node list
cmd+=(-r "$all_nodes")

# --- Debug output ---
echo "Running scanner trigger command:"
printf '%s ' "${cmd[@]}"
echo

# --- Execute ---
"${cmd[@]}"
