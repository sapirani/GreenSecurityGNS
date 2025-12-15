#!/usr/bin/env python3
# reducer_char_freq.py
import sys

current_char = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    ch, cnt = line.split("\t", 1)
    cnt = int(cnt)

    if current_char == ch:
        current_count += cnt
    else:
        if current_char is not None:
            print(f"{current_char}\t{current_count}")
        current_char = ch
        current_count = cnt

if current_char is not None:
    print(f"{current_char}\t{current_count}")
