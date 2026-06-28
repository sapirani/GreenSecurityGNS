import subprocess
import argparse

KB = 1024
MB = 1024 * KB
GB = 1024 * MB

DEFAULT_FILE_SIZE_GB = 1.0
DEFAULT_LEN_OF_WORD = 5
DEFAULT_FILE_NAME = "input.txt"

def hdfs_file_exists(path: str) -> bool:
    return subprocess.run(
        ["hadoop", "fs", "-test", "-e", path]
    ).returncode == 0


def write_to_hdfs(
        words_count: int,
        len_word: int,
        f_name: str,
        *,
        overwrite: bool = False,
        skip_if_exists: bool = False
):

    if hdfs_file_exists(f_name):
        if skip_if_exists:
            print(f"{f_name} already exists in HDFS. Skipping.")
            return

        if overwrite:
            subprocess.run(
                ["hadoop", "fs", "-rm", "-f", f_name],
                check=True,
            )

    subprocess.run(
        f"tr -dc 'a-z' < /dev/urandom | fold -w {len_word} | head -n {words_count} | hadoop fs -put - {f_name}",
        shell=True,
        check=True
    )


def write_to_file(words_count: int, len_word: int, f_name: str):
    with open(f_name, "wb") as f:
        subprocess.run(
            f"tr -dc 'a-z' < /dev/urandom | fold -w {len_word} | head -n {words_count}",
            stdout=f,
            shell=True,
            check=True
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate random lowercase words")
    parser.add_argument("-l", "--word_length", type=int, default=DEFAULT_LEN_OF_WORD,
                        help="Length of each word (default: 5)")
    parser.add_argument("-s", "--size_gb", type=float, default=DEFAULT_FILE_SIZE_GB,
                        help="Total output size in GB")
    parser.add_argument("-o", "--output_path", type=str, default=DEFAULT_FILE_NAME,
                        help="Output file")
    parser.add_argument(
        "-L", "--to_local",
        action="store_true",
        default=False,
        help="Stream output directly into HDFS or locally (default: HDFS)"
    )

    group = parser.add_mutually_exclusive_group()

    group.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the HDFS file if it already exists.",
    )

    group.add_argument(
        "--skip-if-exists",
        action="store_true",
        help="Do nothing if the HDFS file already exists.",
    )

    args = parser.parse_args()
    size_bytes = int(args.size_gb * GB)
    num_of_words = int(size_bytes / (args.word_length + 1))   # one character is required for a new line

    if args.to_local:
        write_to_file(num_of_words, args.word_length, args.output_path)
    else:
        write_to_hdfs(
            num_of_words, args.word_length, args.output_path,
            overwrite=args.overwrite, skip_if_exists=args.skip_if_exists
        )
    print(f"{num_of_words} random words written to {args.output_path if args.to_local else 'HDFS: ' + args.output_path}")

