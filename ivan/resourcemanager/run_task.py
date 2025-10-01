import subprocess
from hadoop_job_config import HadoopJobConfig


# Example usage:
if __name__ == "__main__":
    # Build parser from model defaults
    parser = HadoopJobConfig().to_argparse()

    parser.add_argument(
        "-dr", "--dry-run",
        action="store_true",
        default=False,
        help="Print the Hadoop command and exit"
    )

    args = parser.parse_args()

    config = HadoopJobConfig.from_argparse(args)

    if args.dry_run:
        print(config)
    else:
        subprocess.run(config.get_hadoop_job_args(), check=True)