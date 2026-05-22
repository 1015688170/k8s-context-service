# Kubernetes Context Service

Kubernetes Context Service is a read-only FastAPI service for n8n alert diagnosis workflows. It receives alert context from n8n, queries live Kubernetes context, and returns JSON that is easy for n8n and AI diagnosis steps to consume.

The service uses in-cluster Kubernetes configuration first. For local debugging, it falls back to your local kubeconfig.

## API

```text
POST /api/v1/k8s/context
```

Request:

```json
{
  "cluster": "prod-k3s",
  "namespace": "default",
  "pod": "demo-api-xxx",
  "alert_type": "k8s_pod_crashloop"
}
```

The response includes:

- Pod status and container states
- Previous logs with `previous=true`, `tail_lines=100`, and timestamps
- Current logs with `tail_lines=100` and timestamps
- Recent Pod events, newest first, up to 50 events
- Deployment status discovered through Pod -> ReplicaSet -> Deployment owner references
- Restart count summary
- Per-call errors without failing the whole response

## Local Run

```bash
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```

## Docker Build

```bash
docker build -t k8s-context-service:v1 .
```

## K3s Import Image

```bash
docker save k8s-context-service:v1 -o k8s-context-service-v1.tar
sudo k3s ctr images import k8s-context-service-v1.tar
```

## Deploy

```bash
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
```

## Test

```bash
kubectl port-forward -n ai-alert svc/k8s-context-service 8000:8000
```

```bash
curl -X POST http://127.0.0.1:8000/api/v1/k8s/context \
  -H "Content-Type: application/json" \
  -d '{
    "cluster": "prod-k3s",
    "namespace": "default",
    "pod": "demo-api-xxx",
    "alert_type": "k8s_pod_crashloop"
  }'
```

## RBAC

The provided RBAC is read-only and grants only `get`, `list`, and `watch` for:

- `pods`
- `pods/log`
- `events`
- `nodes`
- `deployments`
- `replicasets`

It does not grant `create`, `delete`, `patch`, or `update`.

## Runtime Limits

The default limits are:

- `PREVIOUS_LOG_TAIL_LINES=50`
- `CURRENT_LOG_TAIL_LINES=30`
- `LOG_TAIL_LINES`: optional compatibility override for both previous and current logs
- `MAX_LOG_CHARS=20000`
- `MAX_EVENTS=20`

You can override these values with environment variables in the Deployment if needed.
