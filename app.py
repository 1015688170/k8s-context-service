from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI
from kubernetes import client, config
from kubernetes.client import ApiException
from pydantic import BaseModel, Field


app = FastAPI(title="Kubernetes Context Service", version="1.0.0")

LOG_TAIL_LINES = int(os.getenv("LOG_TAIL_LINES", "100"))
MAX_LOG_CHARS = int(os.getenv("MAX_LOG_CHARS", "20000"))
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "50"))
SERVICEACCOUNT_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"

_core_v1: Optional[client.CoreV1Api] = None
_apps_v1: Optional[client.AppsV1Api] = None
_api_client: Optional[client.ApiClient] = None


class K8sContextRequest(BaseModel):
    cluster: str = Field(..., description="Cluster name from Alertmanager/n8n")
    namespace: str = Field(..., description="Kubernetes namespace")
    pod: str = Field(..., description="Pod name")
    alert_type: Optional[str] = Field(None, description="Alert type or alert name")


def load_kubernetes_clients() -> Tuple[client.CoreV1Api, client.AppsV1Api]:
    global _core_v1, _apps_v1, _api_client

    if _core_v1 is not None and _apps_v1 is not None:
        return _core_v1, _apps_v1

    loaded_incluster = False
    try:
        config.load_incluster_config()
        loaded_incluster = True
    except config.ConfigException:
        config.load_kube_config()

    configuration = client.Configuration.get_default_copy()
    _api_client = client.ApiClient(configuration)
    if loaded_incluster:
        add_incluster_authorization_header(_api_client)

    _core_v1 = client.CoreV1Api(_api_client)
    _apps_v1 = client.AppsV1Api(_api_client)
    return _core_v1, _apps_v1


def add_incluster_authorization_header(api_client: client.ApiClient) -> None:
    # kubernetes==36.0.0 may load the token but produce empty auth_settings(),
    # which makes requests reach the API server as system:anonymous. Setting the
    # header explicitly keeps in-cluster auth reliable across client versions.
    try:
        with open(SERVICEACCOUNT_TOKEN_PATH, "r", encoding="utf-8") as token_file:
            token = token_file.read().strip()
    except OSError:
        return

    if token:
        api_client.default_headers["Authorization"] = f"Bearer {token}"


@app.on_event("startup")
def startup() -> None:
    # Load early so configuration problems show up in pod logs, while endpoint
    # handling still records config errors in the response if startup did fail.
    try:
        load_kubernetes_clients()
    except Exception as exc:
        print(f"failed to initialize kubernetes client: {exc}")


@app.get("/healthz")
def healthz() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/k8s/context")
def get_k8s_context(payload: K8sContextRequest) -> Dict[str, Any]:
    errors: List[Dict[str, Any]] = []

    response: Dict[str, Any] = {
        "cluster": payload.cluster,
        "namespace": payload.namespace,
        "pod": payload.pod,
        "alert_type": payload.alert_type,
        "pod_status": {},
        "previous_logs": "",
        "current_logs": "",
        "events": [],
        "deployment_status": {
            "found": False,
            "reason": "deployment lookup not attempted",
        },
        "metrics_summary": {
            "restart_count": 0,
        },
        "errors": errors,
    }

    try:
        core_v1, apps_v1 = load_kubernetes_clients()
    except Exception as exc:
        errors.append(error_from_exception("load_config", exc))
        response["deployment_status"] = {
            "found": False,
            "reason": "kubernetes client configuration failed",
        }
        return response

    pod = safe_read_pod(core_v1, payload.namespace, payload.pod, errors)
    container_names: List[str] = []

    if pod is not None:
        pod_status = build_pod_status(pod)
        response["pod_status"] = pod_status
        response["metrics_summary"]["restart_count"] = sum(
            container.get("restart_count", 0)
            for container in pod_status.get("containers", [])
        )
        container_names = [
            container.get("name")
            for container in pod_status.get("containers", [])
            if container.get("name")
        ]
        response["deployment_status"] = find_deployment_status(
            apps_v1=apps_v1,
            namespace=payload.namespace,
            pod=pod,
            errors=errors,
        )
    else:
        response["deployment_status"] = {
            "found": False,
            "reason": "pod lookup failed; deployment owner chain unavailable",
        }

    response["previous_logs"] = get_pod_logs(
        core_v1=core_v1,
        namespace=payload.namespace,
        pod_name=payload.pod,
        container_names=container_names,
        previous=True,
        errors=errors,
    )
    response["current_logs"] = get_pod_logs(
        core_v1=core_v1,
        namespace=payload.namespace,
        pod_name=payload.pod,
        container_names=container_names,
        previous=False,
        errors=errors,
    )
    response["events"] = list_pod_events(
        core_v1=core_v1,
        namespace=payload.namespace,
        pod_name=payload.pod,
        errors=errors,
    )

    return response


def safe_read_pod(
    core_v1: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    errors: List[Dict[str, Any]],
) -> Optional[client.V1Pod]:
    try:
        return core_v1.read_namespaced_pod(name=pod_name, namespace=namespace)
    except Exception as exc:
        errors.append(error_from_exception("read_namespaced_pod", exc))
        return None


def build_pod_status(pod: client.V1Pod) -> Dict[str, Any]:
    metadata = pod.metadata
    spec = pod.spec
    status = pod.status

    containers = []
    for container_status in status.container_statuses or []:
        containers.append(
            {
                "name": container_status.name,
                "image": container_status.image,
                "ready": bool(container_status.ready),
                "restart_count": int(container_status.restart_count or 0),
                "state": serialize_k8s_model(container_status.state),
                "last_state": serialize_k8s_model(container_status.last_state),
            }
        )

    return {
        "name": metadata.name,
        "namespace": metadata.namespace,
        "node_name": spec.node_name if spec else None,
        "phase": status.phase if status else None,
        "pod_ip": status.pod_ip if status else None,
        "host_ip": status.host_ip if status else None,
        "start_time": isoformat(status.start_time if status else None),
        "labels": metadata.labels or {},
        "owner_references": serialize_k8s_model(metadata.owner_references or []),
        "containers": containers,
        "conditions": serialize_k8s_model(status.conditions or []),
    }


def get_pod_logs(
    core_v1: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    container_names: List[str],
    previous: bool,
    errors: List[Dict[str, Any]],
) -> str:
    logs: List[str] = []
    targets = container_names or [None]

    for container_name in targets:
        try:
            log_text = core_v1.read_namespaced_pod_log(
                name=pod_name,
                namespace=namespace,
                container=container_name,
                previous=previous,
                tail_lines=LOG_TAIL_LINES,
                timestamps=True,
            )
            if log_text:
                if container_name:
                    logs.append(f"===== container: {container_name} =====\n{log_text}")
                else:
                    logs.append(log_text)
        except Exception as exc:
            errors.append(
                error_from_exception(
                    "read_namespaced_pod_log",
                    exc,
                    extra={
                        "container": container_name,
                        "previous": previous,
                    },
                )
            )

    return truncate("\n\n".join(logs), MAX_LOG_CHARS)


def list_pod_events(
    core_v1: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    errors: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    try:
        event_list = core_v1.list_namespaced_event(
            namespace=namespace,
            field_selector=f"involvedObject.name={pod_name}",
        )
    except Exception as exc:
        errors.append(error_from_exception("list_namespaced_event", exc))
        return []

    events = sorted(event_list.items or [], key=event_sort_value, reverse=True)

    return [build_event(event) for event in events[:MAX_EVENTS]]


def build_event(event: client.CoreV1Event) -> Dict[str, Any]:
    involved_object = event.involved_object
    return {
        "type": event.type,
        "reason": event.reason,
        "message": truncate(event.message or "", 4000),
        "count": event.count or 0,
        "first_timestamp": isoformat(event.first_timestamp),
        "last_timestamp": isoformat(event.last_timestamp),
        "involved_object": {
            "kind": involved_object.kind if involved_object else None,
            "name": involved_object.name if involved_object else None,
            "namespace": involved_object.namespace if involved_object else None,
        },
    }


def event_sort_value(event: client.CoreV1Event) -> float:
    value = (
        event.last_timestamp
        or event.event_time
        or event.metadata.creation_timestamp
    )
    if value is None:
        return 0.0
    if hasattr(value, "timestamp"):
        return float(value.timestamp())
    return 0.0


def find_deployment_status(
    apps_v1: client.AppsV1Api,
    namespace: str,
    pod: client.V1Pod,
    errors: List[Dict[str, Any]],
) -> Dict[str, Any]:
    replica_set_name = find_owner_name(pod.metadata.owner_references or [], "ReplicaSet")
    if not replica_set_name:
        return {
            "found": False,
            "reason": "pod has no ReplicaSet ownerReference",
        }

    try:
        replica_set = apps_v1.read_namespaced_replica_set(
            name=replica_set_name,
            namespace=namespace,
        )
    except Exception as exc:
        errors.append(
            error_from_exception(
                "read_namespaced_replica_set",
                exc,
                extra={"replica_set": replica_set_name},
            )
        )
        return {
            "found": False,
            "reason": "failed to read ReplicaSet",
            "replica_set": replica_set_name,
        }

    deployment_name = find_owner_name(
        replica_set.metadata.owner_references or [],
        "Deployment",
    )
    if not deployment_name:
        return {
            "found": False,
            "reason": "ReplicaSet has no Deployment ownerReference",
            "replica_set": replica_set_name,
        }

    try:
        deployment = apps_v1.read_namespaced_deployment(
            name=deployment_name,
            namespace=namespace,
        )
    except Exception as exc:
        errors.append(
            error_from_exception(
                "read_namespaced_deployment",
                exc,
                extra={"deployment": deployment_name, "replica_set": replica_set_name},
            )
        )
        return {
            "found": False,
            "reason": "failed to read Deployment",
            "replica_set": replica_set_name,
            "deployment": deployment_name,
        }

    status = deployment.status
    spec = deployment.spec
    return {
        "found": True,
        "name": deployment.metadata.name,
        "namespace": deployment.metadata.namespace,
        "replicas": spec.replicas if spec else None,
        "ready_replicas": status.ready_replicas or 0,
        "available_replicas": status.available_replicas or 0,
        "updated_replicas": status.updated_replicas or 0,
        "conditions": serialize_k8s_model(status.conditions or []),
    }


def find_owner_name(owner_references: List[client.V1OwnerReference], kind: str) -> Optional[str]:
    for owner in owner_references:
        if owner.kind == kind:
            return owner.name
    return None


def serialize_k8s_model(value: Any) -> Any:
    if _api_client is not None:
        return _api_client.sanitize_for_serialization(value)
    return client.ApiClient().sanitize_for_serialization(value)


def isoformat(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def truncate(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    return value[:max_chars] + "\n...<truncated>"


def error_from_exception(
    action: str,
    exc: Exception,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    error: Dict[str, Any] = {"action": action}

    if isinstance(exc, ApiException):
        error.update(
            {
                "type": "ApiException",
                "status": exc.status,
                "reason": exc.reason,
                "body": truncate(exc.body or "", 4000),
            }
        )
    else:
        error.update(
            {
                "type": exc.__class__.__name__,
                "message": str(exc),
            }
        )

    if extra:
        error.update(extra)
    return error
