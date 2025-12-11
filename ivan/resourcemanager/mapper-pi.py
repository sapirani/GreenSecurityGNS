#!/usr/bin/python3
import sys
import random

# Mapper: Generate random points in unit square and emit 1 if inside unit circle
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    # Read number of random sample points to generate for this mapper.
    # Each point is used to estimate π by checking whether it falls inside unit-circle.
    # Larger values give accurate estimate but increase runtime.
    try:
        num_points = int(line)
    except ValueError:
        num_points = 1_000_000  # TODO: invalid input should raise an error instead of silently using a default

    inside_count = 0  # Number points that fall inside the unit-circle (x^2 + y^2 <= 1)
    for _ in range(num_points):
        x = random.random()
        y = random.random()
        if x * x + y * y <= 1.0:
            inside_count += 1

    # Emit total points and inside points as key-value
    print(f"pi_estimate\t{num_points}\t{inside_count}")