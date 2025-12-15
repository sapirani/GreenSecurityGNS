#!/usr/bin/env python3
# reducer_line_stats.py
import sys

total_lines = 0
total_chars = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    key, n_lines, n_chars = line.split("\t", 2)
    total_lines += int(n_lines)
    total_chars += int(n_chars)

if total_lines > 0:
    avg_len = total_chars / total_lines
    print(f"lines\t{total_lines}")
    print(f"chars\t{total_chars}")
    print(f"avg_len\t{avg_len}")
