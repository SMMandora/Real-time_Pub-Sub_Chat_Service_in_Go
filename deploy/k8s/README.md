# Kubernetes manifests

Deploys the chat stack: gateway (autoscaled), worker, and dev backing services
(Redis, Postgres, Jaeger).

## Build & load images

The Deployments reference `chat-gateway:latest` and `chat-worker:latest` with
`imagePullPolicy: IfNotPresent`. Build them from the repo root and make them
available to your cluster (example for kind/minikube):

```bash
docker build --target gateway -t chat-gateway:latest .
docker build --target worker  -t chat-worker:latest .
kind load docker-image chat-gateway:latest chat-worker:latest   # or: minikube image load ...
```

## Apply

```bash
kubectl apply -f deploy/k8s/config.yaml
kubectl apply -f deploy/k8s/backing.yaml      # dev-only Redis/Postgres/Jaeger
kubectl apply -f deploy/k8s/worker.yaml
kubectl apply -f deploy/k8s/gateway.yaml
```

The gateway is exposed via a `LoadBalancer` Service on port 80 → container 8080.

## Autoscaling

`gateway.yaml` includes a `HorizontalPodAutoscaler` (autoscaling/v2) that scales on:

- **CPU** — `averageUtilization: 70`.
- **Active connections per pod** — a `Pods` metric on `chat_active_connections`
  with `averageValue: 8000` (scale out before the ~10k/pod target).

The custom metric requires **prometheus-adapter** to expose
`chat_active_connections` (already on the gateway's `/metrics`) through the
custom-metrics API. Install Prometheus + prometheus-adapter and add a rule
mapping `chat_active_connections` to a `pods` metric, e.g.:

```yaml
rules:
  - seriesQuery: 'chat_active_connections'
    resources: { overrides: { namespace: {resource: namespace}, pod: {resource: pod} } }
    name: { matches: "chat_active_connections", as: "chat_active_connections" }
    metricsQuery: 'avg(chat_active_connections) by (<<.GroupBy>>)'
```

Verify the metric is available to the HPA:

```bash
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/default/pods/*/chat_active_connections"
kubectl get hpa gateway
```

## Production notes

`backing.yaml` is dev-only (no Postgres PVC, single Redis). In production use a
managed Postgres / Redis (or operators with persistence + HA), real Secrets
(sealed-secrets / external-secrets), and an Ingress/Gateway API in front of the
gateway Service with WebSocket support and sticky-enough L4 routing.
