# vcluster-gitops-watcher Helm chart

The chart installs `vcluster-gitops-watcher` with the RBAC it needs to read
`VirtualClusterInstance` objects (and Kargo `Promotion` objects) cluster-wide and
to patch Argo CD cluster Secrets and Applications in the Argo CD namespaces. It
can also install the optional `vcluster-wakeup-proxy`.

## Install from GHCR

```bash
helm upgrade --install vcluster-gitops-watcher \
  oci://ghcr.io/loft-demos/charts/vcluster-gitops-watcher \
  --version 1.3.0-rc.0 \
  --namespace argocd
```

The chart version selects the image tags by default. Override the image
repositories when installing a chart published from a fork:

```bash
--set watcher.image.repository=ghcr.io/OWNER/vcluster-gitops-watcher \
--set proxy.image.repository=ghcr.io/OWNER/vcluster-wakeup-proxy
```

Namespaced RBAC is created in `argocd.namespace` (and in
`argocd.applicationNamespace` / `argocd.clusterSecretNamespace` when they
differ), not in the release namespace, so the chart can be installed in its own
namespace.

## Enable wake requests

Create a Secret holding a vCluster Platform access key, then point the watcher
at the Platform API:

```bash
kubectl -n argocd create secret generic vcluster-platform-token \
  --from-literal=token=<platform-access-key>

helm upgrade --install vcluster-gitops-watcher \
  oci://ghcr.io/loft-demos/charts/vcluster-gitops-watcher \
  --version 1.3.0-rc.0 \
  --namespace argocd \
  --set watcher.wake.upstreamBase=https://platform.example.com \
  --set watcher.wake.existingSecret=vcluster-platform-token
```

To route wakes through the bundled proxy instead, which treats transient
`502`/`504` responses as accepted and can wait for the tenant cluster API:

```bash
  --set proxy.enabled=true \
  --set proxy.upstreamBase=https://platform.example.com \
  --set watcher.wake.useProxy=true \
  --set watcher.wake.existingSecret=vcluster-platform-token
```

The proxy forwards the watcher's `Authorization` header upstream, so the token
Secret is still required with `useProxy`.

## Important values

| Value | Default | Description |
|---|---|---|
| `argocd.namespace` | `argocd` | Default namespace for Argo CD resources |
| `argocd.applicationNamespace` | `argocd.namespace` | Namespace of matching `Application` objects |
| `argocd.clusterSecretNamespace` | `argocd.namespace` | Namespace of Argo CD cluster Secrets |
| `argocd.clusterSecretNameTemplates` | v2 and v1 templates | Ordered cluster name templates, see the main README |
| `argocd.api.base` | empty | Optional Argo CD REST API discovery for hosted Argo CD |
| `argocd.api.existingSecret` | empty | Secret with the Argo CD API token (`tokenKey`) and optional CA (`caKey`) |
| `watcher.image.repository` | `ghcr.io/loft-demos/vcluster-gitops-watcher` | Watcher image repository |
| `watcher.image.tag` | chart `appVersion` | Optional independent image tag |
| `watcher.image.digest` | empty | Optional immutable digest; takes precedence over tag |
| `watcher.pollInterval` | `2s` | `VirtualClusterInstance` poll interval |
| `watcher.projectNamespacePrefixes` | `[p-, loft-p-]` | Prefixes used when no `loft.sh/project` label is present |
| `watcher.patchApplicationHealth` | `true` | Patch non-Kargo app health while the destination sleeps |
| `watcher.kargo.enabled` | `true` | Grant read access to Kargo `Promotion` objects |
| `watcher.wake.upstreamBase` | empty | vCluster Platform base URL; empty disables wake requests |
| `watcher.wake.useProxy` | `false` | Send wake requests to the bundled proxy Service |
| `watcher.wake.existingSecret` | empty | Secret holding the Platform access key (`tokenKey`) |
| `watcher.wake.updateVCILastActivity` | `false` | Patch VCI sleep `lastActivity` after a wake; adds the status RBAC |
| `watcher.extraEnv` | `[]` | Any other watcher variable from the main README |
| `proxy.enabled` | `false` | Install `vcluster-wakeup-proxy` |
| `proxy.upstreamBase` | empty | Required when the proxy is enabled |
| `proxy.wakeReady.timeout` | `30s` | Wait for the woken tenant cluster API before acknowledging |
| `proxy.refreshSecret.namespace` | empty | Enables post-wake Argo CD cluster Secret refresh |
| `proxy.refreshSecret.nameTemplate` | empty | For example `loft-{project}-vcluster-{virtualcluster}` |

`watcher.replicaCount` must remain `1`: the watcher has no leader election, so
the Deployment also uses the `Recreate` strategy to avoid two watchers running
during a rollout.

The cluster-secret pause path requires Argo CD `v3.4.0-rc1` or newer.
