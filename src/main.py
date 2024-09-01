import os
import sys
import json
import time
import queue
import signal
import argparse
import contextlib
import multiprocessing

from kubernetes.utils.create_from_yaml import FailToCreateError

from internal.utils import (
    get_status_code,
    create_signal_handler,
    create_signal_handler,
    cleanup_processes,
)
from internal.logger import get_logger
from internal.workers import (
    JobManager,
    JobPodLogger,
    KubernetesClient,
)


REGION = os.environ.get("REGION", "us-east-1")
CLUSTER_NAME = os.environ.get("CLUSTER_NAME", None)

logger = get_logger(__name__)


def start_job(
    job_manager: JobManager,
    job_name: str,
    job_id: str,
    manifest_path: str,
    namespace: str,
    running_jobs: list,
) -> None:
    """Start a job in the cluster.

    Args:
        job_manager: Job manager object.
        job_name: Name of the job.
        job_id: ID of the job.
        manifest_path: Path to the manifest file.
        namespace: Namespace of the job.
        running_jobs: List of running jobs.
    """
    try:
        job_manager.create_job(manifest_path, job_name, job_id, namespace=namespace)
        running_jobs.append(job_name)
    except FailToCreateError as e:
        for api_exception in e.api_exceptions:
            try:
                error_details = json.loads(api_exception.body)
                if error_details.get("code") == 409:
                    logger.debug(f"Job {job_name} is already running.")
                    running_jobs.append(job_name)
                    break
            except json.JSONDecodeError:
                logger.error("Failed to decode API exception body")
                raise
        else:
            raise
    except Exception as e:
        logger.error(f"An error occurred while creating job {job_name}: {e}")
        sys.exit(1)


def start_listeners(
    job_manager: JobManager,
    pod_logger: JobPodLogger,
    job_name: str,
    namespace: str,
    processes: list,
) -> None:
    """Start listeners for the job and its pods.

    Args:
        job_manager: Job manager object.
        pod_logger: Job pod logger object.
        job_name: Name of the job.
        namespace: Namespace of the job.
        processes: List of processes.
    """
    pods = job_manager.get_job_pods(job_name, namespace=namespace)

    job_listener_process = multiprocessing.Process(
        target=job_manager.listen_to_job, args=(job_name, namespace)
    )

    pod_logger_process = multiprocessing.Process(
        target=pod_logger.stream_logs_from_pod, args=(pods[0], namespace)
    )

    job_listener_process.start()
    pod_logger_process.start()
    processes.extend((job_listener_process.pid, pod_logger_process.pid))


def main_loop(
    job_manager: JobManager,
    job_name: str,
    namespace: str,
    running_jobs: list,
    processes: list,
    ttl_seconds_after_finished: int,
):
    """Main loop to monitor the job and its pods.

    Args:
        job_manager: Job manager object.
        job_name: Name of the job.
        namespace: Namespace of the job.
        running_jobs: List of running jobs.
        processes: List of processes.
        ttl_seconds_after_finished: Time to wait before exiting after job finishes.
    """
    failure_detected_time = None
    job_failed = False

    while True:
        sys.stdout.flush()

        with contextlib.suppress(queue.Empty):
            pod_log = pod_log_queue.get_nowait()
            logger.info(pod_log)

        with contextlib.suppress(queue.Empty):
            exception = job_exception_queue.get_nowait()
            logger.error(f"Exception occurred: {exception}")

            if not job_failed:
                job_failed = True
                failure_detected_time = time.time()

        with contextlib.suppress(queue.Empty):
            status = job_status_queue.get_nowait()
            status_code = get_status_code(status)

            if status_code is not None:
                if status_code == 0:
                    logger.info(f"Job {job_name} completed successfully.")
                    cleanup_processes(
                        job_manager, running_jobs, processes, namespace=namespace
                    )
                    sys.exit(0)

                if not job_failed:
                    job_failed = True
                    failure_detected_time = time.time()
        if (
            job_failed
            and (time.time() - failure_detected_time) >= ttl_seconds_after_finished
        ):
            logger.error(f"Job {job_name} failed.")
            cleanup_processes(job_manager, running_jobs, processes, namespace=namespace)
            sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manifest_path",
        type=str,
        required=True,
        help="Path to the manifest file",
    )
    parser.add_argument(
        "--namespace",
        type=str,
        default="default",
        help="Namespace of the job",
    )
    parser.add_argument(
        "--job_name",
        type=str,
        required=True,
        help="Job name",
    )
    parser.add_argument(
        "--job_id",
        type=str,
        required=True,
        help="Job ID",
    )
    args = parser.parse_args()
    manifest_path = args.manifest_path
    namespace = args.namespace
    job_name = args.job_name
    job_id = args.job_id
    running_jobs = []
    processes = []

    job_status_queue = multiprocessing.Queue()
    job_exception_queue = multiprocessing.Queue()
    pod_log_queue = multiprocessing.Queue()

    k8s_client = KubernetesClient(REGION, CLUSTER_NAME)
    job_manager = JobManager(k8s_client, job_status_queue, job_exception_queue)
    pod_logger = JobPodLogger(k8s_client, pod_log_queue)
    job_name = job_manager.construct_job_name(job_name)
    ttl = job_manager.ttl_seconds_after_finished

    start_job(job_manager, job_name, job_id, manifest_path, namespace, running_jobs)
    start_listeners(job_manager, pod_logger, job_name, namespace, processes)

    signal.signal(
        signal.SIGTERM,
        create_signal_handler(
            job_manager, running_jobs, processes, namespace=namespace
        ),
    )

    main_loop(
        job_manager,
        job_name,
        namespace,
        running_jobs,
        processes,
        ttl,
    )
