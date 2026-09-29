#!/usr/bin/env python3

import sys

pattern = "abc"

for line in sys.stdin:
    line = line.rstrip("\n")

    if pattern in line:
        print(f"{line}\t")
