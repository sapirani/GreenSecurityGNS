#!/usr/bin/env python3

import sys

# shuffle phase already sorted that keys, so we just need to print them
for line in sys.stdin:
    line = line.rstrip("\n")

    if line:
        key, _ = line.split("\t", 1)
        print(key)
