#!/usr/bin/env python3
# mapper_line_stats.py
import sys

for line in sys.stdin:
    line = line.rstrip("\n")
    length = len(line)
    # key is a constant so all partial stats go to one reducer group
    print(f"STATS\t1\t{length}")
