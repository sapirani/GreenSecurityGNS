#!/usr/bin/python3
import sys

# Reducer: Aggregate total points and inside points across mappers
total_points = 0
total_inside = 0

for line in sys.stdin:
    line = line.strip()
    if line.startswith("pi_estimate"):
        parts = line.split('\t')
        if len(parts) == 3:
            points = int(parts[1])
            inside = int(parts[2])
            total_points += points
            total_inside += inside

if total_points > 0:
    pi_estimate = 4.0 * total_inside / total_points
    print(f"Estimated Pi: {pi_estimate}")
    print(f"Using {total_points} total points")
