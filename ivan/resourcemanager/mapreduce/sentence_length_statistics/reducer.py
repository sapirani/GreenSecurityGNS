#!/usr/bin/env python3
# reducer_sentence_stats.py
import sys
from collections import defaultdict

current_word = None
lengths = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    word, length = line.split('\t', 1)
    length = int(length)

    if current_word == word:
        lengths.append(length)
    else:
        if current_word and lengths:
            # Output: first_word<TAB>avg_len<TAB>min_len<TAB>max_len<TAB>count
            avg = sum(lengths) / len(lengths)
            print(f"{current_word}\t{avg:.1f}\t{min(lengths)}\t{max(lengths)}\t{len(lengths)}")

        current_word = word
        lengths = [length]

# Don't forget the last word
if current_word and lengths:
    avg = sum(lengths) / len(lengths)
    print(f"{current_word}\t{avg:.1f}\t{min(lengths)}\t{max(lengths)}\t{len(lengths)}")
