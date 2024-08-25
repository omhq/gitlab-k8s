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

from internal.utils import get_status_code, create_signal_handler, cleanup_processes
from internal.logger import get_logger
from internal.workers import (
    JobManager,
    JobPodLogger,
    KubernetesClient,
)


REGION = os.environ.get("REGION", "us-east-1")
CLUSTER_NAME = os.environ.get("CLUSTER_NAME", None)
NAMESPACE = os.environ.get("NAMESPACE", "default")
JOB_NAME = os.environ.get("JOB_NAME")

logger = get_logger(__name__)
job_status_queue = multiprocessing.Queue()
job_exception_queue = multiprocessing.Queue()
pod_log_queue = multiprocessing.Queue()
k8s_client = KubernetesClient(REGION, CLUSTER_NAME)
job_manager = JobManager(k8s_client, job_status_queue, job_exception_queue)
pod_logger = JobPodLogger(k8s_client, pod_log_queue)
running_jobs = []
processes = []


def main(manifest_path: str, namespace: str = "default") -> None:
    """Main function to create a job and listen to its status and logs.

    Args:
        manifest_path (str): Path to the manifest file.
        namespace (str): Namespace of the job.
    """
    job_name = job_manager.construct_job_name(JOB_NAME)
    ttl_seconds_after_finished = job_manager.ttl_seconds_after_finished

    try:
        job_manager.create_job(manifest_path, job_name, namespace=namespace)
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

    pods = job_manager.get_job_pods(job_name, namespace=namespace)
    default_handler = signal.getsignal(signal.SIGTERM)

    signal.signal(signal.SIGTERM, signal.SIG_IGN)

    job_listener_process = multiprocessing.Process(
        target=job_manager.listen_to_job, args=(job_name, namespace)
    )

    pod_logger_process = multiprocessing.Process(
        target=pod_logger.stream_logs_from_pod, args=(pods[0], namespace)
    )

    job_listener_process.start()
    pod_logger_process.start()
    processes.append(job_listener_process.pid)
    processes.append(pod_logger_process.pid)

    signal.signal(signal.SIGTERM, default_handler)

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
                    cleanup_processes()
                    sys.exit(0)

                if not job_failed:
                    job_failed = True
                    failure_detected_time = time.time()
        if (
            job_failed
            and (time.time() - failure_detected_time) >= ttl_seconds_after_finished
        ):
            logger.error(f"Job {job_name} failed.")
            cleanup_processes()
            sys.exit(1)


if __name__ == "__main__":
    signal.signal(
        signal.SIGTERM,
        create_signal_handler(
            job_manager, running_jobs, processes, namespace=NAMESPACE
        ),
    )

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m",
        "--manifest_path",
        type=str,
        required=True,
        help="Path to the manifest file",
    )
    args = parser.parse_args()
    manifest_path = args.manifest_path
    main(manifest_path, namespace=NAMESPACE)
