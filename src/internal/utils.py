import sys
import json
import psutil

from kubernetes.client import V1JobStatus
from kubernetes.client.rest import ApiException

from internal.logger import get_logger


logger = get_logger(__name__)


def get_status_code(status: V1JobStatus) -> int:
    """Returns 0 if the job is successful, 1 if the job has failed,
    and None if the job is still active.

    Args:
        status (V1JobStatus): The status of the job.

    Returns:
        int: 0 for success, 1 for failure, None for active.
    """
    status = status.to_dict()

    if status.get("succeeded") is not None:
        return 0
    elif status.get("failed") is not None:
        return 1
    else:
        return None


def terminate_process(pid: int) -> None:
    """Terminate a process by its PID.

    Args:
        pid (int): Process ID of the process to terminate.
    """
    logger.debug(f"Process {pid} terminating.")

    try:
        process = psutil.Process(pid)
        process.terminate()

        try:
            process.wait(timeout=2)
            logger.debug(f"Process {pid} terminated successfully.")
        except psutil.TimeoutExpired:
            logger.debug(
                f"Timeout expired while waiting for process {pid} to terminate. Sending SIGKILL."
            )
            process.kill()
            process.wait(timeout=2)
            logger.debug(f"Process {pid} killed successfully.")
    except psutil.NoSuchProcess:
        logger.info(f"No process found with PID {pid}.")
    except psutil.AccessDenied:
        logger.info(f"Access denied when trying to terminate process {pid}.")
    except psutil.TimeoutExpired:
        logger.info(f"Timeout expired while waiting for process {pid} to terminate.")
    except Exception as e:
        logger.info(f"An error occurred: {e}")


def cleanup_processes(
    job_manager: object, running_jobs: list, processes: list, namespace: str = "default"
) -> None:
    """Cleanup processes and jobs.

    Args:
        job_manager: Job manager object.
        running_jobs: List of running jobs.
        processes: List of processes.
        namespace: Namespace of the job.
    """
    for job_name in running_jobs:
        logger.debug(f"Deleting job {job_name}...")
        try:
            job_manager.delete_job(job_name, namespace=namespace)
            logger.debug(f"Deleted job {job_name}.")
        except ApiException as e:
            try:
                error_details = json.loads(e.body)
                if error_details.get("code") == 404:
                    logger.debug(f"Job {job_name} is already deleted, or not found.")
            except json.JSONDecodeError:
                logger.error("Failed to decode API exception body")
            else:
                logger.error(f"An error occurred while deleting job {job_name}: {e}")

    for process in processes:
        logger.debug(f"Terminating process {process}...")
        terminate_process(process)


def create_signal_handler(
    job_manager: object, running_jobs: list, processes: list, namespace: str = "default"
) -> callable:
    def signal_handler(signum, frame):
        """Signal handler to terminate the job in the cluster.

        Args:
            job_manager: Job manager object.
            signum: Signal number.
            frame: Current stack frame.
            namespace: Namespace of the job.

        Returns:
            callable: Signal handler function.
        """
        logger.debug(f"Received signal {signum}, terminating processes...")
        cleanup_processes(job_manager, running_jobs, processes, namespace=namespace)
        sys.exit(0)

    return signal_handler
