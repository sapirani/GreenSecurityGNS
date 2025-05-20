#!/usr/bin/python3

import sys

# Input comes from STDIN (standard input)
import time

for line in sys.stdin:
    # Remove leading and trailing whitespace
    line = line.strip()
    # Split the line into words
    words = line.split()
    # Output the word with a count of 1
    time.sleep(10)
    for word in words:
        print(f"{word}\t1")
