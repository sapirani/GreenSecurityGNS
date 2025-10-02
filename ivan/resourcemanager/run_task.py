import subprocess
from hadoop_job_config import HadoopJobConfig


if __name__ == "__main__":
    # Build parser from model defaults
    parser = HadoopJobConfig().to_argparse()

    parser.add_argument(
        "-p", "--print_command_only",
        action="store_true",
        default=False,
        help="Print the Hadoop command and exit"
    )

    args = parser.parse_args()

    config = HadoopJobConfig.from_argparse(args)

    if args.print_command_only:
        print(config)
    else:
        subprocess.run(config.get_hadoop_job_args(), check=True)
