# Release Notes

## 2.1.4-rc.1

The watcher now leaves tenant clusters with private nodes alone, and three fixes found in a review: a VCI could take another VCI's cluster Secret, an Application that stayed OutOfSync woke its VCI every time it fell asleep, and a refresh on an awake but paused cluster never reached Argo CD.

### New: tenant clusters with private nodes are left alone

A tenant cluster with private nodes cannot sleep, so pausing Argo CD for it saves nothing, and both deadlocks fixed in 2.1.2-rc.1 and 2.1.3-rc.1 hit private-nodes clusters. The watcher now reads `privateNodes.enabled` from the VCI's vcluster.yaml (`status.virtualCluster.helmRelease.values` for a templated VCI, `spec.template.helmRelease.values` otherwise) and does nothing for such a VCI: no pause, no health patching, no wake, no sleep after sync.

- If an earlier version left `skip-reconcile` on the cluster Secret of such a VCI, the watcher removes it once and logs `removed ... : VCI <ns>/<name> is not managed (...)`.
- The new `gitops-watcher.loft-demos.github.io/manage` annotation overrides the detection: `"false"` opts any VCI out, `"true"` keeps a private-nodes VCI managed.

### Fixed: a VCI could match another VCI's cluster Secret

When a VCI's own cluster Secret was missing, for example in the seconds between creating the tenant cluster and its Argo CD registration, the last-resort name match accepted any Secret whose name shared its first 40 characters. Two VCIs with similar names (`nv-gpu-opera-test` and `nv-gpu-operator-test`), or any two VCIs in a project whose name is 19 characters or longer, matched each other's Secret. The watcher then paused, un-paused, woke, or patched the health of the wrong cluster.

- A Secret whose `loft.sh/vcluster-instance-*` labels name another VCI is never matched to this one, at any step.
- A truncated name now matches only the exact name the platform's `SafeConcatNameMax` produces from this VCI's expected name. The old shared-prefix match remains only for Secrets without instance labels, and only when the VCI's whole name falls inside the compared prefix.

### Fixed: an Application that stays OutOfSync woke its VCI after every sleep

An Application that never got back in sync (manual sync, a failing sync, drift Argo CD keeps reporting) counted as wake work again each time its VCI fell asleep, because the wake retry treated "never tried" as "due". The VCI was woken, went back to sleep, and was woken again.

- A wake is now retried only if the watcher attempted it and the VCI has not reached Ready since. A new OutOfSync revision still wakes the VCI as before.

### Fixed: a refresh on an awake but paused cluster never reached Argo CD

A refresh requested on a ready VCI the watcher had paused as idle (a UI refresh, a Git webhook, or the refresh that made the watcher wake it when application health patching is off) stayed pending behind `skip-reconcile`.

- A new refresh request, or the one the VCI was woken for, now un-pauses a ready destination for the existing `WATCH_READY_REFRESH_GRACE` window, logging `removed ... : applications <names> have a refresh pending`. With the grace set to `0` the behavior is unchanged.

## 2.1.3-rc.1

One fix, completing the 2.1.2-rc.1 fix for tenant clusters paused before Argo CD finished with them.

### Fixed: a destination re-paused mid rollout stayed Progressing forever

2.1.2-rc.1 kept a destination un-paused until Argo CD had reconciled each Application once. One reconcile is often mid rollout: Argo CD records `Progressing` while a Deployment or DaemonSet comes up, and only a later reconcile moves it to `Healthy`. The watcher then saw no sync intent and no `OutOfSync` revision, re-paused the destination as idle, and the Application stayed `Progressing` with its last health message (for example `Waiting for daemon set ... rollout to finish`) long after every pod was ready. Stack tasks waiting for `Healthy` timed out.

- An Application whose health is `Progressing`, or whose sync operation is still `Running`, now counts as work for a ready destination and for one in an unknown state, alongside never-reconciled Applications. The watcher logs `removed ... : applications <names> are still rolling out` when that is why it un-pauses.
- `Progressing` health that the watcher itself patched while the vCluster was waking (the `wakingHealthMessage`) does not count.
- As before, none of this wakes a sleeping VCI, and a destination is re-paused once its Applications are `Healthy` (or `Degraded`, `Suspended` and so on) with no operation running.
- To recover a destination stuck by this bug, upgrade. The watcher un-pauses it on its next poll, Argo CD reconciles, and the Application moves on from `Progressing`.

## 2.1.2-rc.1

One fix, for tenant clusters that register with Argo CD before their first reconcile.

### Fixed: a new tenant cluster could stay paused before its first Argo CD reconcile

A tenant cluster that registers with Argo CD while it is still coming up was re-paused about a second later as an "idle ready" destination, and it stayed paused. The watcher keeps a ready destination un-paused only for sync intent, an `OutOfSync` revision or a Kargo promotion, and Argo CD writes all of those by reconciling. A new Application on a paused destination is never reconciled, so it never showed work, so the destination was never un-paused. Every Argo CD Application on the cluster, including Stack tasks and the Fleet Observability collector, sat with an empty status. Stack tasks then failed with `RefreshApplicationFailed`. Refreshing, invalidating the Argo CD cluster cache and restarting the application controller did not help, because Argo CD was correctly skipping a cluster marked `skip-reconcile`. Only a manual sync, which sets a sync intent, released it.

- An Application that Argo CD has never reconciled (no `reconciledAt`, no sync or health status, no operation state) now counts as work for a ready destination, and for one in an unknown state. The watcher does not pause it, and un-pauses it if it is already paused, logging `removed ... : applications <names> have never been reconciled`. `sync` mode also waits for it before putting the VCI back to sleep.
- Once every Application has reconciled, the destination is re-paused as before.
- It is not a wake signal: a sleeping VCI with a new Application stays asleep until one of the existing wake signals fires.

## 2.1.1-rc.1

Two fixes. The `CreateContainerConfigError` fix was prepared as 2.1.0-rc.2, which was never published, so it ships here.

### Fixed: a refresh-triggered wake could leave Argo CD paused before it reconciled

When something set `argocd.argoproj.io/refresh` on an Application for a sleeping tenant cluster (vCluster Platform does this when a Stack task refreshes its app), the watcher woke the VCI and un-paused its cluster Secret once it was ready. On the next poll, about two seconds later, it saw no sync or revision work and re-applied `skip-reconcile`, before Argo CD had reconciled the app. The refresh then stayed pending behind the pause, and Stack tasks that waited on it failed with `RefreshApplicationFailed`. Repeating the same refresh did nothing, because the watcher had already seen that refresh request.

- After the watcher un-pauses a ready destination, a pending `refresh` annotation now keeps it un-paused until Argo CD clears the annotation, for at most `WATCH_READY_REFRESH_GRACE` (default `2m`, chart `watcher.readyRefreshGrace`). The same window also stops `sync` mode from putting the VCI back to sleep mid-refresh.
- Refresh annotations are still edge-triggered wake signals, not persistent work: one that outlives the grace no longer holds the pause off, and the watcher logs `re-paused ... with a refresh still pending after <grace>`.
- To recover an app stuck by this bug, upgrade, then refresh it again. The new watcher process has no record of the earlier refresh request, so it wakes the VCI again.

### Fixed: the chart's default install failed with CreateContainerConfigError

The chart sets `runAsNonRoot: true`, but the watcher and proxy images ran as the distroless user named `nonroot`. Kubernetes can only verify `runAsNonRoot` against a numeric user, so with the chart defaults the pod never started:

```log
container has runAsNonRoot and image has non-numeric user (nonroot), cannot verify user is non-root
```

- The chart now sets `runAsUser: 65532` and `runAsGroup: 65532` (distroless `nonroot`) in the default `podSecurityContext` for both the watcher and the proxy.
- Both images now declare `USER 65532:65532`, so `runAsNonRoot` also works outside the chart.

No configuration changes are needed. If you worked around this by setting `podSecurityContext.runAsUser` / `runAsGroup` yourself, you can remove those values.

## 2.1.0-rc.1

This release gives you control over which tenant clusters the watcher wakes, adds a mode that wakes a tenant cluster for a deploy and puts it straight back to sleep afterwards, and replaces the managed wake access key with short-lived tokens that need far fewer permissions.

### Highlights

- **Choose which VCIs to wake.** Wake every VCI by default and opt individual ones out, or wake none by default and opt individual ones in.
- **Sleep after sync.** A new `sync` mode wakes a sleeping VCI for a deploy and puts it straight back to sleep once the deploy is done, unless someone else used it meanwhile.
- **Least-privilege wake user.** The wake request now works for a user with no access inside tenant clusters, and the README shows a `gitops-watcher` user and project role with only the two permissions a wake needs.
- **Short-lived wake tokens.** `WATCH_WAKE_ACCESS_KEY_USER` / `_TEAM` no longer create a long-lived AccessKey. The watcher now impersonates that Platform user to get a token per tenant cluster that expires after minutes, and no longer needs permission to create AccessKeys for any user.

### Choosing Which VCIs to Wake

- New `WATCH_WAKE_DEFAULT`: `enabled` (the default, unchanged behavior) wakes every sleeping VCI with pending GitOps work; `disabled` wakes none; `sync` wakes, deploys, and puts the VCI back to sleep.
- The per-VCI annotation `gitops-watcher.loft-demos.github.io/wake` overrides it with `"true"`, `"false"`, or `"sync"`.
- A VCI that is not woken is still paused while it sleeps and shows `Suspended` in Argo CD. Its pending syncs run once it is woken some other way.
- An invalid annotation value is logged once and the global mode applies.
- The annotation prefix is configurable with `WATCH_ANNOTATION_PREFIX` for forks.
- In the chart: `watcher.wake.default` and `watcher.annotationPrefix`.

### Sleep After Sync

In `sync` mode (`WATCH_WAKE_DEFAULT=sync` or the annotation `gitops-watcher.loft-demos.github.io/wake: "sync"`), the watcher wakes a sleeping VCI for pending GitOps work, then waits until the deploy is done. That means every matching Application is `Synced` and `Healthy` with nothing pending, and any Kargo Stage is `Ready` (verified). If nobody else used the tenant cluster since the wake, the watcher sets `sleepmode.loft.sh/force: "true"` and it goes straight back to sleep. If someone else did, it is left to the normal sleep timer.

- `WATCH_SLEEP_AFTER_SYNC_TIMEOUT` (default `15m`): give up and leave the normal sleep timer in charge if the deploy is not done by then.
- `WATCH_SLEEP_AFTER_SYNC_SETTLE` (default `1m`): minimum time after the VCI is ready before the deploy can count as done.
- `WATCH_WAKE_SUBJECT`: the wake credential's sleep-mode subject, derived automatically from the managed wake access key. Use a dedicated wake user so the watcher can tell its own activity from real users.
- In the chart: `watcher.wake.default: sync` and `watcher.wake.sleepAfterSync.*`.

### Short-Lived Wake Tokens

In 2.0.0-rc.1, setting `WATCH_WAKE_ACCESS_KEY_USER` (or `_TEAM`) made the watcher create and maintain a long-lived `vcluster-gitops-watcher-wake` AccessKey. That needed `create` on `accesskeys.storage.loft.sh`, which Kubernetes RBAC cannot limit by name, so the watcher's ServiceAccount could mint keys for any Platform user, including admins.

The same settings now work differently:

- For each wake, the watcher requests a token kubeconfig for that one VCI (`virtualclusterinstances/<name>/kubeconfig`) while impersonating the configured user or team, and sends the wake request with its token.
- Each token covers a single tenant cluster and expires after `WATCH_WAKE_TOKEN_TTL` (default `10m`). vCluster Platform creates the AccessKey behind it and deletes it once it expires.
- Tokens are cached per tenant cluster, renewed shortly before they expire, and replaced after a `401`.
- The watcher needs only `impersonate` on that one user and its `loft:user:<name>` group (or `loft:team:<name>`), both limited by name, and `create` on `virtualclusterinstances/kubeconfig`. It no longer creates, reads, or patches AccessKeys.
- `sync` mode still derives the wake subject (`loft:user:<name>`) automatically.
- In the chart: `watcher.wake.accessKey.user` / `.team` are unchanged; `watcher.wake.accessKey.tokenTTL` is new. `WATCH_WAKE_ACCESS_KEY_NAME` / `_PROJECTS` and `watcher.wake.accessKey.name` / `.projects` are no longer used.

### Wake Request and Wake User

- The watcher now sends `GET /kubernetes/project/<project>/virtualcluster/<name>/version` to wake a tenant cluster, instead of `POST /kubernetes/project/<project>/virtualcluster/<name>`. vCluster Platform forwards the wake request into the tenant cluster once it is ready, and a `POST /` there needed permissions inside the tenant cluster. Every authenticated user may read `/version`, so the wake user needs none.
- A wake request that times out while vCluster Platform holds it for the starting tenant cluster now counts as triggered instead of failed. Platform starts the wake as soon as the request arrives. This also lets `sync` mode track wakes that take longer than `WATCH_WAKE_TIMEOUT`.
- The bundled `vcluster-wakeup-proxy` treats this `GET .../version` as a wake request too, so `useProxy` keeps its accepted-status handling.
- New README section "Wake User": a `gitops-watcher` Platform user with no password and a `gitops-watcher-wake` role granting only `use` on `virtualclusterinstances` and `create` on `virtualclusterinstances/kubeconfig`. Bound through the user's `spec.clusterRoles`, it covers every project with no per-project setup; adding the user to selected projects' `spec.members` instead limits it to those projects.

### Upgrade Notes

- **No behavior change by default.** Without `WATCH_WAKE_DEFAULT` or the wake annotation, the watcher wakes every sleeping VCI with pending GitOps work, as in 2.0.0-rc.1.
- **New RBAC for `sync` mode:** `patch` on `virtualclusterinstances` and `get` on Kargo `stages`. The chart grants the VCI `patch` whenever wake requests are configured, since any VCI can opt in with the annotation, and grants `get` on Stages whenever Kargo support is enabled. The rules are also in `deploy/watcher-rbac.yaml`.
- **If you used the managed wake access key from 2.0.0-rc.1:** no configuration change is needed. Once the new release is running, delete the old AccessKey with `kubectl delete accesskeys.storage.loft.sh vcluster-gitops-watcher-wake`. The chart replaces the AccessKey permissions with the impersonation rules automatically. With the plain manifests, swap the commented `vcluster-gitops-watcher-wake-accesskey` ClusterRole for `vcluster-gitops-watcher-wake-tokens` from `deploy/watcher-rbac.yaml`.
- **The wake user must be able to use the tenant clusters it wakes**, as before. The tokens are scoped to that user's own Platform permissions. To replace a broad user such as `admin`, follow "Wake User" in the README.
- **Wake requests changed shape.** Anything that inspects the watcher's wake traffic, such as an upstream proxy or audit rule matching `POST .../virtualcluster/<name>`, needs to match `GET .../virtualcluster/<name>/version` as well.
- **Correction to the 2.0.0-rc.1 notes:** vCluster Platform ignores Argo CD integration traffic for sleep mode from v4.10.6, v4.11.0, and v4.12.0, not only v4.12.0. The wake token and `ignore-user-agents` guidance in those notes applies to all of these versions.

### Documentation

- New README section "Choosing Which VCIs to Wake" with the mode table and an example, and "Sleep After Sync".
- New README section "Wake User", with standalone manifests in `examples/wake-user`.
- The README's "Managed Wake Access Key" section is replaced by "Short-Lived Wake Tokens".
- The example manifests document `WATCH_WAKE_DEFAULT`, the `sync` mode RBAC, and the wake token RBAC.

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
