#!/usr/bin/env python3

import sys

for line in sys.stdin:
    line = line.rstrip("\n")

    if line:
        # Hadoop Streaming expects: key<TAB>value
        print(f"{line}\t")
