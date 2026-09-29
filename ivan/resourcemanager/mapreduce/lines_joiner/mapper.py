#!/usr/bin/env python3

import sys

MATCH_LENGTH = 3

for line in sys.stdin:
    line = line.rstrip("\n")

    if len(line) < MATCH_LENGTH:
        continue

    prefix = line[:MATCH_LENGTH]
    suffix = line[-MATCH_LENGTH:]

    # Emit both relationships:
    # suffix → line as a potential LEFT line
    # prefix → line as a potential RIGHT line

    print(f"{suffix}\tLEFT\t{line}")
    print(f"{prefix}\tRIGHT\t{line}")
