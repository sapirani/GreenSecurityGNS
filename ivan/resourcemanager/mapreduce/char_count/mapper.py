#!/usr/bin/env python3
# mapper_char_freq.py
import sys

for line in sys.stdin:
    line = line.rstrip("\n")
    for ch in line:
        if ch.isspace():
            continue
        print(f"{ch}\t1")
