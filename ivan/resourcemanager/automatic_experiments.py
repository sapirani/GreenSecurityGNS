import subprocess
from argparse import ArgumentParser
from time import sleep
from typing import Optional

from automatic_experiments_parameters import experiments_config, scanner_trigger_sender
from trigger_sender import TriggerSender
from jobs_configurator import ExperimentMode
import logging

logger = logging.getLogger(__name__)


def setup_logger():
    # TODO: UNIFY THIS FUNCTION WITH THE APPLICATION FLOW LOGGER FROM THE OTHER REPO
    logger.setLevel(logging.DEBUG)  # Or DEBUG, WARNING, etc.
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s %(message)s'))
    if not logger.handlers:
        logger.addHandler(handler)


def handle_sequential_mode(shared_session_id: Optional[str]):
    """
    If shared session ID is provided:
        This function:
        1. starts the resource measurement code across all nodes.
        2. runs all Hadoop jobs one by one (as defined by the user).
        3. stops the resource measurement code across all nodes.
    Otherwise:
        For each Hadoop job, this function:
        1. generates a session id, tailored for the current Hadoop job
        2. starts the resource measurement code across all nodes.
        3. runs the job
        4. stops the resource measurement code across all nodes.
    """
    if shared_session_id:
        scanner_trigger_sender.start_measurement(session_id=shared_session_id)

    executed_successfully = True
    all_experiment_configurations = experiments_config.all_experiments_configurations()
    for experiment_index, experiment_config in enumerate(all_experiment_configurations):
        user_selected_fields = experiments_config.user_selected_fields(experiment_config)

        session_id = None
        # generate a session ID according to the user's configuration and start measurements
        if not shared_session_id:
            session_id = TriggerSender.generate_session_id(
                generate_session_from=user_selected_fields
            )
            scanner_trigger_sender.start_measurement(session_id=session_id)
            print(f"**************** Session ID: {session_id} ****************")

        # run experiments
        try:
            print(
                f"Running a new job ({experiment_index + 1} / {len(all_experiment_configurations)}):\n"
                f"{experiment_config}\n"
            )
            print(experiment_config.format_user_selection(user_selected_fields))

            subprocess.run(experiment_config.get_hadoop_job_args(), check=True)

            print(f"\nJob has terminated successfully.{'' if shared_session_id else 'Session ID: ' + session_id}\n")
            print(experiment_config)
        except subprocess.CalledProcessError as e:
            logger.warning(
                f"The execution of a Hadoop job encountered an error.\n"
                f"The job:\n{experiment_config}\nThe error: {e}\n"
            )
            executed_successfully = False
        except FileNotFoundError:
            logger.error("It seems like Hadoop is not installed on this device")
            raise

        if not shared_session_id:
            scanner_trigger_sender.stop_measurement()
        sleep(experiments_config.sleep_between_launches)

    if shared_session_id:
        print(f"Terminating resource measurements. Session ID: {shared_session_id}")
        scanner_trigger_sender.stop_measurement()

    return executed_successfully


def handle_parallel_mode(shared_session_id: Optional[str]):
    """
    This function starts the resource measurement code across all nodes.
    Then, run Hadoop jobs in parallel (as defined by the user).
    Eventually, it stops the resource measurement code across all nodes.
    """
    jobs_processes = []
    scanner_trigger_sender.start_measurement(session_id=shared_session_id)
    executed_successfully = True

    all_experiment_configurations = experiments_config.all_experiments_configurations()

    for experiment_index, experiment_config in enumerate(all_experiment_configurations):
        try:
            user_selected_fields = experiments_config.user_selected_fields(experiment_config)
            print(
                f"Running a new job ({experiment_index + 1} / {len(all_experiment_configurations)}):\n"
                f"{experiment_config}\n"
            )
            print(experiment_config.format_user_selection(user_selected_fields))

            jobs_processes.append((experiment_config, subprocess.Popen(experiment_config.get_hadoop_job_args())))
        except FileNotFoundError:
            logger.error("It seems like Hadoop is not installed on this device")
            raise
        sleep(experiments_config.sleep_between_launches)

    # TODO: we might think of an enhanced technique for waiting processes as they terminate (not necessary for now).
    for experiment_config, job_process in jobs_processes:
        job_return_code = job_process.wait()
        if job_return_code == 0:
            logger.info(f"Job has terminated successfully:\n{experiment_config}")
        else:
            executed_successfully = False
            logger.warning(
                f"Hadoop job exited with unexpected exit code: {job_return_code}.\n"
                f"Job configuration:\n{experiment_config}"
            )

    print(f"Terminating resource measurements. {'Session ID:' + shared_session_id if shared_session_id else ''}")
    scanner_trigger_sender.stop_measurement()
    return executed_successfully


def run_tasks(mode: ExperimentMode, should_keep_output_directories: bool, shared_session_id: Optional[str]):
    try:
        executed_successfully = False
        if mode == ExperimentMode.SEQUENTIAL:
            executed_successfully = handle_sequential_mode(shared_session_id)
        elif mode == ExperimentMode.PARALLEL:
            executed_successfully = handle_parallel_mode(shared_session_id)

        print(f"\nFinished automatic experiments {'successfully' if executed_successfully else 'unsuccessfully'}")

    # Terminate the measurements no matter what (even if the user pressed CTRL+C)
    finally:
        try:
            scanner_trigger_sender.stop_measurement()
            if not should_keep_output_directories:
                experiments_config.remove_outputs()
        except Exception:
            logger.critical("An unexpected error occurred upon stopping measurements")


def main(print_configurations_only: bool, should_keep_output_directories: bool, shared_session_id: Optional[str]):
    if print_configurations_only:
        print(f"\n{experiments_config}\n")
    else:
        run_tasks(experiments_config.mode, should_keep_output_directories, shared_session_id)


if __name__ == '__main__':
    setup_logger()

    parser = ArgumentParser(description="A Python wrapper for Hadoop job configuration")
    parser.add_argument(
        "-p", "--print_configurations_only",
        action="store_true",
        default=False,
        help="Print the Hadoop experiments' configuration and exit"
    )

    parser.add_argument(
        "-k", "--keep_output_directories",
        action="store_true",
        default=False,
        help="Whether to keep output directories, or remove them when program is terminating"
    )

    parser.add_argument(
        "-s", "--shared_session_id",
        type=str,
        default=None,
        help="A single session id for all experiments. By default, in sequential mode, each experiment defines"
             "a custom session ID, where in parallel mode a default session ID is chosen."
    )

    args = parser.parse_args()

    main(args.print_configurations_only, args.keep_output_directories, args.shared_session_id)
