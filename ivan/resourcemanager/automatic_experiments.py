import subprocess
from time import sleep

from automatic_experiments_parameters import experiments_config, scanner_trigger_sender
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


# TODO: SUPPORT SEQUENTIAL MODE AND PARALLEL MODE
# TODO: SUPPORT STARTING THE SCANNER AND STOPPING IT WHEN SCAN IS FINISHED
# TODO: SUPPORT STOPPING THE SCANNER UPON RECEIVING A KILL SIGNAL
# TODO: SUPPORT A SESSION ID BASED ON THE RECEIVED INPUTS FROM THE USER
#  (use aliases from HadoopJobConfiguration for the fields that the user have modified solely)
# TODO: ADD AN OPTION TO REMOVE THE OUTPUT DIRECTORIES ENTIRELY AT THE END OF THE MEASUREMENT SESSION


def handle_sequential_mode():
    """
    This function starts the resource measurement code across all nodes.
    Then
    """
    try:
        for experiment_config in experiments_config.all_experiments_configurations():
            scanner_trigger_sender.start_measurement()
            try:
                print("Running a job:")
                print(experiment_config)
                subprocess.run(experiment_config.get_hadoop_job_args(), check=True)
            except subprocess.CalledProcessError as e:
                logger.warning(
                    f"Executing Hadoop job encountered an error.\n"
                    f"The job:\n{experiment_config}\nThe error: {e}\n"
                )
            except FileNotFoundError:
                logger.error("It seems like Hadoop is not installed on this device")
                raise
            scanner_trigger_sender.stop_measurement()
            sleep(experiments_config.sleep_between_launches)
    finally:
        try:
            scanner_trigger_sender.stop_measurement()
        except Exception:
            pass


def handle_parallel_mode():
    jobs_processes = []
    scanner_trigger_sender.start_measurement()
    try:
        for experiment_config in experiments_config.all_experiments_configurations():
            try:
                print("Running a job:")
                print(experiment_config)
                jobs_processes.append((experiment_config, subprocess.Popen(experiment_config.get_hadoop_job_args())))

            except FileNotFoundError:
                logger.error("It seems like Hadoop is not installed on this device")
                raise
            sleep(experiments_config.sleep_between_launches)

        # Note: we might think of an enhanced technique for waiting processes as they terminate (not necessary for now).
        for experiment_config, job_process in jobs_processes:
            job_return_code = job_process.wait()
            if job_return_code != 0:
                logger.warning(
                    f"Hadoop job exited with unexpected exit code: {job_return_code}.\n"
                    f"Job configuration:\n{experiment_config}"
                )

    finally:
        scanner_trigger_sender.stop_measurement()


if __name__ == '__main__':
    setup_logger()

    if experiments_config.print_configurations_only:
        print(experiments_config)

    elif experiments_config.mode == ExperimentMode.SEQUENTIAL:
        handle_sequential_mode()

    elif experiments_config.mode == ExperimentMode.PARALLEL:
        handle_parallel_mode()

    print("Finished automatic experiments")
