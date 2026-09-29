#!/usr/bin/env python3

import sys

current_key = None
left_lines = []
right_lines = []

for line in sys.stdin:
    key, side, text = line.rstrip("\n").split("\t", 2)

    if key != current_key:
        # Process previous group
        for left in left_lines:
            for right in right_lines:
                print(f"{left}{right[3:]}")

        current_key = key
        left_lines = []
        right_lines = []

    if side == "LEFT":
        left_lines.append(text)
    else:
        right_lines.append(text)

# Process final group
for left in left_lines:
    for right in right_lines:
        print(f"{left}{right[3:]}")
