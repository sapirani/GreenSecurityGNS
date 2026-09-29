#!/usr/bin/env python3

import sys

current_palindrome = None
count = 0

for line in sys.stdin:
    palindrome, value = line.rstrip("\n").split("\t", 1)
    value = int(value)

    if palindrome == current_palindrome:
        count += value
    else:
        if current_palindrome is not None:
            print(f"{current_palindrome}\t{count}")

        current_palindrome = palindrome
        count = value

if current_palindrome is not None:
    print(f"{current_palindrome}\t{count}")
