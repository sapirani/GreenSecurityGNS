#!/usr/bin/python3
import sys
import random

# Mapper: Generate random points in unit square and emit 1 if inside unit circle
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    # Read number of points per mapper (e.g., from first line or fixed)
    try:
        num_points = int(line)
    except ValueError:
        num_points = 1000000  # Default if input invalid

    inside_count = 0
    for _ in range(num_points):
        x = random.random()
        y = random.random()
        if x * x + y * y <= 1.0:
            inside_count += 1

    # Emit total points and inside points as key-value
    print(f"pi_estimate\t{num_points}\t{inside_count}")
