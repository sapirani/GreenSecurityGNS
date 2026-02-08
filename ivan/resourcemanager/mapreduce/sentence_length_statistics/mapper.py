#!/usr/bin/env python3
# mapper_sentence_stats.py
import sys
import re

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    # Split into sentences (simple heuristic: .!? followed by space/capital)
    sentences = re.split(r'[.!?]+\s+', line)

    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) < 3:  # Skip very short fragments
            continue

        first_word = sentence.split()[0].lower() if sentence.split() else ""
        if first_word:
            # Output: first_word<TAB>length
            print(f"{first_word}\t{len(sentence)}")
