#!/usr/bin/env python3

import sys

for line in sys.stdin:
    line = line.rstrip("\n")

    if line and line == line[::-1]:
        print(f"{line}\t1")
