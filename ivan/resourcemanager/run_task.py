import subprocess
from hadoop_job_config import HadoopJobConfig


if __name__ == "__main__":
    # Build parser from model defaults
    parser = HadoopJobConfig.to_argparse()

    parser.add_argument(
        "-p", "--print_command_only",
        action="store_true",
        default=False,
        help="Print the Hadoop command and exit"
    )

    args = parser.parse_args()
    hadoop_job_config = HadoopJobConfig.from_argparse(args)

    if args.print_command_only:
        print(hadoop_job_config)
    else:
        subprocess.run(hadoop_job_config.get_hadoop_job_args(), check=True)
