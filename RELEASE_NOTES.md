# Release Notes

## 2.0.0-rc.1

This release makes the watcher match vCluster Platform's v2 ("connector") Argo CD integration exactly, lets it create its own wake credentials, and ships it as a Helm chart. It targets vCluster Platform v4.12.0 and later, where Argo CD can no longer wake a sleeping tenant cluster and the watcher becomes the GitOps wake path.

### Highlights

- **Helm chart.** Install the watcher, and optionally the wakeup proxy, with `helm install vcluster-gitops-watcher oci://ghcr.io/loft-demos/charts/vcluster-gitops-watcher --version 2.0.0-rc.1`. The chart is published to GHCR on every GitHub release, with the chart version and image tags taken from the release tag.
- **Reliable v2 cluster Secret matching.** Fixes v2 tenant clusters on licensed platforms never being paused.
- **Managed wake access key.** The watcher can create and maintain its own vCluster Platform AccessKey for wake requests, so there is no token to create, store, or rotate by hand.

### Cluster Secret Matching

The watcher now resolves each `VirtualClusterInstance` to its Argo CD cluster Secret in this order:

1. The exact name from the VCI annotation `loft.sh/argocd-registered-cluster-name`, which the platform writes after a successful v2 registration, then the configured name templates.
2. The `loft.sh/vcluster-instance-name` and `loft.sh/vcluster-instance-namespace` labels, which both the legacy and v2 integrations set on the Secret.
3. The server URL. The new `WATCH_PLATFORM_HOST` lets the watcher derive the exact server the platform registers (`https://<host>/kubernetes/project/<project>/virtualcluster/<name>`). Servers on matching Applications are tried next.
4. A name fallback for the platform's instance-ID hash suffix and for names truncated to 49 characters.

What this fixes:

- On licensed platforms, v2 cluster names end with a 6-character hash of the platform instance ID (for example `loft-default-virtualcluster-llm-argocd-1a2b3c`). The name templates could not predict it, so the watcher never paused those tenant clusters unless a plain Argo CD Application already targeted them by server.
- Akuity Applications, which target the hashed name by `destination.name`, now match too.
- The name fallback was non-deterministic and accepted the first Secret it found. When several platforms share one Argo CD, it could pause another platform's Secret.

### Managed Wake Access Key

- Set `WATCH_WAKE_ACCESS_KEY_USER` (or `WATCH_WAKE_ACCESS_KEY_TEAM`) and the watcher creates a `storage.loft.sh/v1` AccessKey named `vcluster-gitops-watcher-wake` (`WATCH_WAKE_ACCESS_KEY_NAME`), scoped to `WATCH_WAKE_ACCESS_KEY_PROJECTS` (default `*`), and uses it for every wake request.
- The key never carries `sleepmode.loft.sh/ignore-activity`. The watcher repairs or recreates it when it is deleted, disabled, or edited, on startup or after a wake request is rejected with `401`.
- The watcher only manages an AccessKey it created, and refuses to take over an existing one with the same name.
- In the chart: `watcher.wake.accessKey.user` / `.team` / `.projects`. It cannot be combined with `watcher.wake.existingSecret`.
- Security: this mode needs `create` on `accesskeys.storage.loft.sh`, which Kubernetes RBAC cannot limit by name, so the watcher's ServiceAccount can mint keys for any Platform user. It is opt-in, and providing a token stays supported.

### Upgrade Notes

- **Set `WATCH_PLATFORM_HOST`** (`watcher.platformHost` in the chart). It is optional, but gives an exact server match and is required to tell this platform's Secrets apart when several platforms share one Argo CD.
- **Ambiguous matches are no longer resolved.** When more than one Secret matches by labels or by the name fallback, the watcher now pauses none of them and logs the candidates once, instead of picking one. Setting `WATCH_PLATFORM_HOST` resolves this.
- **Check your wake token.** It must not be the access key the platform created for the Argo CD integration. Since vCluster Platform v4.12.0 that key carries `sleepmode.loft.sh/ignore-activity`, and wake requests made with it fail with a `502` and never wake the tenant cluster. Use a dedicated access key, or the new managed wake access key.
- **`sleepmode.loft.sh/ignore-user-agents: argo*` is no longer needed** for platform-registered clusters on vCluster Platform v4.12.0 and later, because the platform already ignores Argo CD integration traffic. It is still the only option for clusters registered with other credentials, and on earlier platform versions.
- New RBAC is only needed for the managed wake access key. The rules are in the chart and, commented out, in `deploy/watcher-rbac.yaml`.

### Documentation

- New README sections on the matching order, the wake token, the managed wake access key, and sleep mode with Argo CD traffic on vCluster Platform v4.12.0 and later. The latter replaces "Reduced Need for `sleepmode.loft.sh/ignore-user-agents`".
- The proxy section notes that Argo CD traffic sent through the proxy with the integration access key cannot wake a tenant cluster.
- New chart README with install examples and a values reference.

## 1.3.0-rc.0

### Large Kubernetes List Reliability

- Kubernetes list requests for VirtualClusterInstances, Argo CD Applications,
  Kargo Promotions, and Argo CD cluster Secrets now use server-side pagination.
- API response bodies are no longer silently truncated at 1 MiB. Responses are
  accepted up to 16 MiB per page/request, and an explicit size error is returned
  if that safety limit is exceeded.
- This fixes continuous `unexpected end of JSON input` reconcile failures once
  Argo CD Application specs and statuses make the ApplicationList exceed 1 MiB.
- Added regression coverage for multi-page Application lists whose combined JSON
  payload exceeds the former limit.

## Dual v1 / v2 Argo CD Integration Support (Watcher)

`vcluster-gitops-watcher` now supports both the legacy (v1) Argo CD integration, where vCluster Platform creates the cluster Secret with a predictable name, and the v2 ("connector") integration, where the platform registers the cluster through the Argo CD REST API and Argo CD auto-generates the Secret `metadata.name`. Detection is automatic; operators do not pick a mode.

### What's Changed in the Watcher

- Cluster Secrets are now discovered by the `argocd.argoproj.io/secret-type=cluster` label and indexed by their decoded `data.name` and `data.server`, instead of being fetched by an exact `metadata.name`. This resolves v2 Secrets whose name is generated by Argo CD while leaving v1 resolution unchanged.
- Expected cluster names are derived from an ordered list of templates via the new `ARGOCD_CLUSTER_SECRET_NAME_TEMPLATES` (default covers both the `virtualcluster` and `vcluster` infixes). Each template is tried both bare and with the v2 connector's `-argocd` suffix (for example `loft-default-virtualcluster-llm-large-argocd`). The singular `ARGOCD_CLUSTER_SECRET_NAME_TEMPLATE` still works and pins a single template. Names truncated by the platform's length cap are matched by prefix as a fallback.
- Applications are matched by **both** `spec.destination.name` (v1 and Akuity v2) and `spec.destination.server` (plain Argo CD v2), unioned and de-duplicated. Server URLs are compared by normalized equality.
- The skip-reconcile pause/wake/refresh lifecycle is unchanged and now operates on each resolved Secret's real `metadata.name`.
- Pause honesty: when no patchable cluster Secret resolves, the watcher no longer claims to pause. An optional Argo CD REST API discovery mode (`ARGOCD_API_*`) can list clusters and Applications for hosted control planes, but it never pauses over the API and limits the watcher to wake/refresh signals in that case.
- RBAC now grants `list`/`watch` on Secrets (in addition to `get`/`patch`) in the cluster-secret namespace.

### Watcher Validation

- Added unit tests for Secret index resolution (v1 infix, v2 infix, server-only match, truncated-prefix fallback, and not-found), destination-server indexing and URL normalization, plain-Argo-CD-v2 and Akuity-v2 reconcile matching, the no-patchable-Secret path, and template configuration precedence. Existing v1 behavior is covered for regressions.

## Wake Request Handling

This release improves how `vcluster-wakeup-proxy` handles wake-triggering virtual cluster requests.

Previously, the proxy behavior was described mainly in terms of converting upstream `502` and `504` responses into success. With this update, the behavior is more intentional: the proxy now treats the wake-triggering request as accepted when the upstream likely initiated the wake-up flow but returned a transient retryable failure before the virtual cluster was ready.

## What's Changed

- Wake-specific handling now applies only to `POST /kubernetes/project/<project>/virtualcluster/<name>`
- Configured retryable upstream statuses such as `502` and `504` are treated as accepted only for that wake request path
- When `SUCCESS_ON_ERROR=true`, retryable early transport failures on the wake path can also be treated as accepted
- Permanent upstream failures such as `401`, `403`, `404`, malformed requests, and non-retryable transport errors are still returned normally
- The proxy now logs upstream response statuses and upstream transport errors to make debugging easier

## Why It Matters

This better matches the real wake-up behavior of sleeping virtual clusters. A wake request may successfully start the wake-up flow even when the upstream returns a temporary error right away. With this change, callers such as Argo can treat that initial wake trigger as accepted without hiding genuine configuration or authorization problems.

## Validation

- Added test coverage for wake acceptance behavior
- Added test coverage for non-wake pass-through behavior
- Added test coverage for retryable and non-retryable transport failures
