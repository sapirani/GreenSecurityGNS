import subprocess
from time import sleep

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

# TODO: ADD AN OPTION TO REMOVE THE OUTPUT DIRECTORIES ENTIRELY AT THE END OF THE MEASUREMENT SESSION
# TODO: ENSURE THAT THE OUTPUT DIRECTORIES DO NOT EXIST BEFORE STARTING MEASUREMENTS (PROBABLY USING PYDANTIC VALIDATOR)
# TODO: SUPPORT A SHARED SESSION-ID IN THE CASE OF SEQUENTIAL MODE
# TODO: ALLOW RANDOM SESSION IDS IN SEQUENTIAL MODE (DO NOT NECESSARILY GENERATE FROM SHORTCUTS)


def handle_sequential_mode():
    # TODO: FINISH THIS COMMENT ACCORDING TO THE SHARED SESSION ID OPTION
    """
    This function starts the resource measurement code across all nodes.
    Then, run Hadoop jobs one by one (as defined by the user).
    Eventually, it stops the resource measurement code across all nodes.
    """
    executed_successfully = True

    try:
        for experiment_config in experiments_config.all_experiments_configurations():
            user_selected_fields = experiments_config.user_selected_fields(experiment_config)
            session_id = TriggerSender.generate_session_id(
                generate_session_from=user_selected_fields
            )
            scanner_trigger_sender.start_measurement(session_id=session_id)
            try:
                print(f"Session ID: {session_id}, running a job\n{experiment_config}\n")
                print(experiment_config.format_user_selection(user_selected_fields))
                subprocess.run(experiment_config.get_hadoop_job_args(), check=True)
            except subprocess.CalledProcessError as e:
                logger.warning(
                    f"The execution of a Hadoop job encountered an error.\n"
                    f"The job:\n{experiment_config}\nThe error: {e}\n"
                )
                executed_successfully = False
            except FileNotFoundError:
                logger.error("It seems like Hadoop is not installed on this device")
                executed_successfully = False
                raise
            scanner_trigger_sender.stop_measurement()
            sleep(experiments_config.sleep_between_launches)

    # Terminate the measurements no matter what (even if the user pressed CTRL+C)
    finally:
        try:
            scanner_trigger_sender.stop_measurement()
            return executed_successfully
        except Exception:
            pass


def handle_parallel_mode():
    """
    This function starts the resource measurement code across all nodes.
    Then, run Hadoop jobs in parallel (as defined by the user).
    Eventually, it stops the resource measurement code across all nodes.
    """
    jobs_processes = []
    scanner_trigger_sender.start_measurement()
    executed_successfully = True
    try:
        for experiment_config in experiments_config.all_experiments_configurations():
            try:
                jobs_processes.append((experiment_config, subprocess.Popen(experiment_config.get_hadoop_job_args())))
            except FileNotFoundError:
                executed_successfully = False
                logger.error("It seems like Hadoop is not installed on this device")
                raise
            sleep(experiments_config.sleep_between_launches)

        # TODO: we might think of an enhanced technique for waiting processes as they terminate (not necessary for now).
        for experiment_config, job_process in jobs_processes:
            job_return_code = job_process.wait()
            if job_return_code != 0:
                executed_successfully = False
                logger.warning(
                    f"Hadoop job exited with unexpected exit code: {job_return_code}.\n"
                    f"Job configuration:\n{experiment_config}"
                )

    # Terminate the measurements no matter what (even if the user pressed CTRL+C)
    finally:
        try:
            scanner_trigger_sender.stop_measurement()
            return executed_successfully
        except Exception:
            pass


def main():
    executed_successfully = True

    if experiments_config.print_configurations_only:
        print()
        print(experiments_config)

    elif experiments_config.mode == ExperimentMode.SEQUENTIAL:
        executed_successfully = handle_sequential_mode()

    elif experiments_config.mode == ExperimentMode.PARALLEL:
        executed_successfully = handle_parallel_mode()

    print(f"\nFinished automatic experiments {'successfully' if executed_successfully else 'unsuccessfully'}")


if __name__ == '__main__':
    setup_logger()
    main()
    # TODO: MOVE PRINT CONFIGURATIONS ONLY HERE AS AN ARGPARSE INSTEAD OF INSIDE THE PYDANTIC MODEL
