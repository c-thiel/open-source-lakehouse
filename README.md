# Building a Secure Open-Source Lakehouse

Hands-on workshop: secure Apache Iceberg tables across multiple query engines using open standards.

You'll learn which OAuth2 flows to use for human and machine users, when to enforce
permissions in the catalog versus the query engine, and how to wire identity providers,
authorization systems, query engines, and the Iceberg REST Catalog together on Kubernetes.

## Components

| Component | Purpose | Namespace |
|-----------|---------|-----------|
| [CloudNativePG](https://cloudnative-pg.io/) | PostgreSQL operator | `cnpg-system` |
| [Sealed Secrets](https://sealed-secrets.netlify.app/) | Encrypt secrets for Git storage | `sealed-secrets` |
| [Envoy Gateway](https://gateway.envoyproxy.io/) | Ingress with TLS termination | `envoy-gateway-system` |
| [SeaweedFS](https://github.com/seaweedfs/seaweedfs) | S3-compatible object storage | `seaweedfs` |
| [Keycloak](https://www.keycloak.org/) | Identity provider (OAuth2) | `keycloak` |
| [Lakekeeper](https://github.com/lakekeeper/lakekeeper) | Iceberg REST Catalog | `lakekeeper` |
| [OpenFGA](https://openfga.dev/) | Authorization system (bundled with Lakekeeper) | `lakekeeper` |
| [Trino](https://trino.io/) | Multi-user query engine | `trino` |
| Spark & PyIceberg | Single-user engines (run locally) | — |

## Prerequisites

- Docker
- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Helm](https://helm.sh/docs/intro/install/)
- [kubeseal](https://github.com/bitnami-labs/sealed-secrets/releases) (Sealed Secrets CLI)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (Python package manager)

## Quick Start

### 1. Create the kind cluster

```bash
kind create cluster --name lakehouse --config kind-cluster.yaml
```

Patch CoreDNS so that `*.localhost` hostnames resolve to the correct in-cluster
services. This allows pods to use the same URLs as your local machine:

```bash
kubectl get configmap coredns -n kube-system -o yaml | \
  sed '/rewrite name.*\.localhost/d' | \
  sed 's/ready/rewrite name keycloak.localhost keycloak-alias.keycloak.svc.cluster.local\n        rewrite name lakekeeper.localhost lakekeeper-alias.lakekeeper.svc.cluster.local\n        rewrite name s3.localhost seaweedfs-alias.seaweedfs.svc.cluster.local\n        rewrite name starrocks.localhost starrocks-alias.starrocks.svc.cluster.local\n        ready/' | \
  kubectl apply -f -

kubectl rollout restart deployment coredns -n kube-system
```

Verify the rewrite works (once SeaweedFS is deployed):

```bash
kubectl run dns-test --rm -it --restart=Never --image=busybox:1.36 -- nslookup keycloak.localhost
```

### Working with namespaces

Throughout this workshop we install each component in its own namespace.
To quickly switch your default namespace, set up this alias:

```bash
alias kns='kubectl config set-context --current --namespace'
_kns() { COMPREPLY=($(compgen -W "$(kubectl get namespaces -o jsonpath='{.items[*].metadata.name}')" -- "${COMP_WORDS[COMP_CWORD]}")); }
complete -F _kns kns
```

Then switch namespaces with:

```bash
kns lakekeeper    # now all kubectl commands target the lakekeeper namespace
kns keycloak      # switch to keycloak
```

You can always check which namespace you're in with:

```bash
kubectl config view --minify --output 'jsonpath={..namespace}'
```

### 2a. Install Core components

Install in order — each step depends on the previous one.

```bash
# CloudNativePG operator
helm dependency build charts/cloudnative-pg
helm upgrade --install cloudnative-pg charts/cloudnative-pg -n cnpg-system --create-namespace

# Wait for CNPG operator to be ready
kubectl wait --for=condition=Available deployment/cloudnative-pg \
  -n cnpg-system --timeout=120s
```

```bash
# Sealed Secrets controller (optional, for secure secrets in git!)
helm dependency build charts/sealed-secrets
helm upgrade --install sealed-secrets charts/sealed-secrets -n sealed-secrets --create-namespace
```

```bash
# Envoy Gateway (ingress with TLS)
helm dependency build charts/envoy-gateway
helm upgrade --install envoy-gateway charts/envoy-gateway -n envoy-gateway-system --create-namespace
```

```bash
# Object storage
helm upgrade --install seaweedfs charts/seaweedfs -n seaweedfs --create-namespace
```

```bash
# Identity provider
helm upgrade --install keycloak charts/keycloak -n keycloak --create-namespace
```

```bash
# Iceberg REST Catalog (includes OpenFGA for authorization)
helm dependency build charts/lakekeeper
helm upgrade --install lakekeeper charts/lakekeeper -n lakekeeper --create-namespace
```

### 2b. Query Engines

```bash
# trino
helm dependency build charts/trino
helm upgrade --install trino charts/trino -n trino --create-namespace
```

```bash
# starrocks
helm upgrade --install starrocks charts/starrocks -n starrocks --create-namespace
```

### 3. Access the services

All services are routed via Envoy Gateway. Trino uses HTTPS (TLS terminated
at the gateway with a self-signed certificate), everything else uses HTTP.

| Service | URL | Credentials |
|---------|-----|-------------|
| Keycloak Admin | https://keycloak.localhost:30443 | admin / admin |
| Lakekeeper UI | http://lakekeeper.localhost:30080 | (via Keycloak OAuth2) |
| Trino | https://trino.localhost:30443 | (via Keycloak OAuth2) |
| SeaweedFS S3 | http://s3.localhost:30080 | `admin` / `adminadmin` |

> **Note:** Keycloak and Trino use a self-signed certificate. You will need to
> accept the certificate warning in your browser on first visit.

### 4. Running scripts locally

The workshop scripts run locally in VSCode outside the cluster, connecting
to services via the URLs above.

```bash
cd scripts
uv sync
```

`*.localhost` resolves to `127.0.0.1` on most systems. If it doesn't work on yours,
add the following to `/etc/hosts`:

```
127.0.0.1 keycloak.localhost lakekeeper.localhost trino.localhost s3.localhost
```

### 5. Teardown

```bash
kind delete cluster --name lakehouse
```

## Repository Structure

```
kind-cluster.yaml          kind cluster config with port mappings
charts/
  cloudnative-pg/          CloudNativePG operator (cnpg-system)
  sealed-secrets/          Bitnami Sealed Secrets controller (sealed-secrets)
  envoy-gateway/           Envoy Gateway + Gateway + TLS (envoy-gateway-system)
  seaweedfs/               SeaweedFS S3-compatible object storage (seaweedfs)
  keycloak/                Keycloak + CNPG Cluster + HTTPRoute (keycloak)
  lakekeeper/              Lakekeeper + OpenFGA + 2x CNPG Cluster + HTTPRoute (lakekeeper)
  trino/                   Trino + Iceberg catalog + HTTPRoute (trino)
```

Each chart is a thin wrapper around an upstream Helm chart (as a dependency), adding
workshop-specific configuration, CNPG-managed PostgreSQL clusters, and Gateway API
routing where needed.
