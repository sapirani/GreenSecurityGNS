#!/usr/bin/env python3

import sys

for line in sys.stdin:
    match, _ = line.rstrip("\n").split("\t", 1)
    print(match)
