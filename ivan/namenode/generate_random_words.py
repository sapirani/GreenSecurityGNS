import random
import string
import sys

NUMBER_OF_WORDS = 10 * (2 ** 20)
LEN_OF_WORD = 5
FILE_NAME = "input.txt"


def generate_word(word_l: int) -> string:
    return "".join(random.choices(string.ascii_lowercase, k=word_l))


def write_to_file(n_words: int, len_word: int, f_name: str):
    with open(f_name, "w") as f:
        for _ in range(n_words):
            f.write(generate_word(len_word) + "\n")


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) > 3:
        num_of_words = int(args[0])
        len_of_word = int(args[1])
        file_name = args[2]
    else:
        num_of_words = NUMBER_OF_WORDS
        len_of_word = LEN_OF_WORD
        file_name = "input.txt"

    write_to_file(num_of_words, len_of_word, file_name)
    print(f"{num_of_words} random words written to {file_name}")
