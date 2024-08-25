import os
import re
import uuid
import yaml
import time
import boto3
import base64
import tempfile
import contextlib
import multiprocessing

from typing import Union
from datetime import timezone, datetime, timedelta
from urllib3.exceptions import ProtocolError
from kubernetes import client, watch
from kubernetes.utils import create_from_dict
from kubernetes.client.rest import ApiException
from botocore import session
from awscli.customizations.eks.get_token import (
    STSClientFactory,
    TokenGenerator,
    TOKEN_EXPIRATION_MINS,
)
from .logger import get_logger


BRANCH = os.getenv("BRANCH", "main")
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

logger = get_logger(__name__)


class JobFailedException(Exception):
    pass


class KubernetesClient:
    def __init__(self, region, cluster_name):
        """Init.

        Args:
            cluster_name: string EKS cluster name.
            region: string, AWS region.
        """
        self.region = region
        self.cluster_name = cluster_name
        self.eks_client = boto3.client("eks", region_name=self.region)
        self.client_factory = STSClientFactory(session.get_session())

        cafile, k8s_client = self.make_k8s_client()
        self.cafile = cafile
        self.k8s_client = k8s_client

        self.core_v1 = client.CoreV1Api(self.k8s_client)
        self.batch_v1 = client.BatchV1Api(self.k8s_client)

    def __del__(self):
        """Delete the CA cert temp file and close k8s connection."""
        with contextlib.suppress(Exception):
            self.k8s_client.close()
        with contextlib.suppress(Exception):
            self.cafile.delete()

    def get_expiration_time(self):
        """Generate expiration time to be used with EKS auth.

        Return:
            Formatted date string.
        """
        token_expiration = datetime.now(timezone.utc) + timedelta(
            minutes=TOKEN_EXPIRATION_MINS
        )
        return token_expiration.strftime("%Y-%m-%dT%H:%M:%SZ")

    def get_token(self, cluster_name: str, role_arn: str = None) -> dict:
        """Generate EKS auth token.

        Args:
            cluster_name: Cluster name to authenticate with.
            role_arn: Role arn to use when generating the token, defaults to None.

        Return:
            Dictionary.
        """
        sts_client = self.client_factory.get_sts_client(
            role_arn=role_arn,
            region_name=self.region,
        )
        token = TokenGenerator(sts_client).get_token(cluster_name)
        return {
            "kind": "ExecCredential",
            "apiVersion": "client.authentication.k8s.io/v1alpha1",
            "spec": {},
            "status": {
                "expirationTimestamp": self.get_expiration_time(),
                "token": token,
            },
        }

    def write_cafile(self, data: str) -> tempfile.NamedTemporaryFile:
        """Save the CA cert to a temp file (working around the Kubernetes client limitations).

        Args:
            data: Base64 encoded cluster CA cert.

        Returns:
            File object containing base64 decoded cluster CA cert.
        """
        fd, path = tempfile.mkstemp()
        with os.fdopen(fd, "wb") as cafile:
            cadata_b64 = data
            cadata = base64.urlsafe_b64decode(cadata_b64)
            cafile.write(cadata)
        return open(path, "rb")

    def make_k8s_client(self) -> Union[tempfile.NamedTemporaryFile, client.ApiClient]:
        """Initialize the k8s ApiClient.

        Returns:
            k8s ApiClient object.
        """
        eks_details = self.eks_client.describe_cluster(name=self.cluster_name)[
            "cluster"
        ]
        ca_file = self.write_cafile(
            eks_details["certificateAuthority"]["data"].encode("utf-8")
        )
        token = self.get_token(self.cluster_name)

        conf = client.Configuration()
        conf.host = eks_details["endpoint"]
        conf.api_key["authorization"] = token["status"]["token"]
        conf.api_key_prefix["authorization"] = "Bearer"
        conf.ssl_ca_cert = ca_file.name
        return ca_file, client.ApiClient(conf)


class JobManager:
    def __init__(
        self,
        k8s_client: KubernetesClient,
        status_queue: multiprocessing.Queue,
        exception_queue: multiprocessing.Queue,
    ):
        self.k8s_client = k8s_client.k8s_client
        self.batch_v1 = k8s_client.batch_v1
        self.core_v1 = k8s_client.core_v1
        self.status_queue = status_queue
        self.exception_queue = exception_queue
        self.ttl_seconds_after_finished = 60

    @staticmethod
    def normalize_job_name(job_name: str) -> str:
        """Normalize job name to Kubernetes standards.

        Args:
            job_name: Job name.

        Returns:
            Normalized job name.
        """
        job_name = job_name.lower()
        job_name = re.sub(r"[^a-z0-9-]", "-", job_name)
        job_name = re.sub(r"^[^a-z0-9]+", "", job_name)
        job_name = re.sub(r"[^a-z0-9]+$", "", job_name)
        return job_name[:51]

    @staticmethod
    def construct_job_name(job_name: str) -> str:
        """Construct a unique job name.

        Args:
            job_name: Job name.

        Returns:
            Unique job name.
        """
        normalized_job_name = JobManager.normalize_job_name(job_name)
        return f"{normalized_job_name}-{str(uuid.uuid4())[:12]}"

    def create_job(
        self, manifest_path: str, job_name: str, namespace: str = "default"
    ) -> None:
        """Submit a namespaced job workload to the cluster.

        Args:
            manifest_path: Path to the job manifest file.
            namespace: The namespace in which to create the job.
        """
        with open(manifest_path, "r") as file:
            job_manifest = yaml.safe_load(file)

        job_manifest["metadata"]["name"] = job_name
        job_manifest["spec"]["template"]["metadata"]["name"] = job_name
        containers = job_manifest["spec"]["template"]["spec"]["containers"]

        self.ttl_seconds_after_finished = job_manifest["spec"].get(
            "ttlSecondsAfterFinished", self.ttl_seconds_after_finished
        )

        for container in containers:
            env_vars = container.get("env", [])
            env_vars.append({"name": "BRANCH", "value": BRANCH})
            env_vars.append({"name": "DEBUG", "value": str(DEBUG)})
            container["env"] = env_vars

        create_from_dict(self.k8s_client, job_manifest, namespace=namespace)

    def get_job(self, name: str, namespace: str = "default") -> client.V1Job:
        """Try to retrieve a namespaced job workload.

        Args:
            name: Job name.
            namespace: Cluster namespace.

        Returns:
            V1Job
        """
        return self.batch_v1.read_namespaced_job(name, namespace)

    def delete_job(self, name: str, namespace: str = "default") -> client.V1Status:
        """Try to delete a namespaced job.

        Args:
            name: Job name.
            namespace: The namespace in which to create the job.

        Returns:
            V1Status
        """
        body = client.V1DeleteOptions(propagation_policy="Background")
        return self.batch_v1.delete_namespaced_job(name, namespace, body=body)

    def listen_to_job(self, job_name: str, namespace: str = "default") -> None:
        """Listen to the job status.

        Args:
            job_name: Job name.
            namespace: Cluster namespace.
        """
        while True:
            time.sleep(5)

            try:
                job = self.get_job(job_name, namespace)
                self.status_queue.put(job.status)
            except ProtocolError as e:
                self.exception_queue.put(e)
            except ApiException as e:
                self.exception_queue.put(e)

    def get_job_pods(
        self, job_name: str, namespace: str = "default", poll_interval: int = 5
    ) -> list:
        """Get the pods associated with a job.

        Args:
            job_name: Job name.
            namespace: Cluster namespace.

        Returns:
            List of pod names.
        """
        label_selector = f"job-name={job_name}"

        while True:
            pods = self.core_v1.list_namespaced_pod(
                namespace, label_selector=label_selector
            )
            if pod_names := [pod.metadata.name for pod in pods.items]:
                return pod_names

            time.sleep(poll_interval)


class JobPodLogger:
    def __init__(
        self,
        k8s_client: KubernetesClient,
        pod_log_queue: multiprocessing.Queue,
    ):
        self.core_v1 = k8s_client.core_v1
        self.pod_log_queue = pod_log_queue

    def check_pod_status(self, pod_name: str, namespace: str) -> str:
        """Check the status of a pod.

        Args:
            pod_name: Pod name.
            namespace: Cluster namespace.

        Returns:
            Pod status.
        """
        return self.core_v1.read_namespaced_pod_status(
            name=pod_name, namespace=namespace
        ).status.phase

    def stream_logs(self, pod_name: str, namespace: str, w: watch.Watch) -> None:
        """Stream logs from a pod.

        Args:
            pod_name: Pod name.
            namespace: Cluster namespace.
            w: Watch object.
        """
        retries = 0
        max_retries = 2

        while retries < max_retries:
            try:
                stream = w.stream(
                    self.core_v1.read_namespaced_pod_log,
                    name=pod_name,
                    namespace=namespace,
                    follow=True,
                    _preload_content=False,
                )

                for log_line in stream:
                    self.pod_log_queue.put(log_line)
            except ProtocolError as e:
                logger.error(
                    f"{type(e).__name__} exception occurred while streaming logs: {e}"
                )
                time.sleep(5)
                retries += 1
                continue
            except Exception as e:
                logger.error(
                    f"An error occurred while streaming logs for pod {pod_name}: {e}"
                )
                time.sleep(5)
                retries += 1
                continue

    def stream_logs_from_pod(self, pod_name: str, namespace: str = "default") -> None:
        """Parallelize pod log streaming instead of streaming logs sequentially for each pod."""
        w = watch.Watch()

        while True:
            pod_status = self.check_pod_status(pod_name, namespace)

            if pod_status == "Running":
                logger.debug(f"Pod {pod_name} is running, starting log stream")
                break

            logger.debug(
                f"Pod {pod_name} is not running yet, current status: {pod_status}"
            )

            time.sleep(5)

        self.stream_logs(pod_name, namespace, w)
