# Wake User

Least-privilege vCluster Platform user for `vcluster-gitops-watcher` wake requests. See [Wake User](../../README.md#wake-user) in the main README for why each permission is needed.

## Files

- [cluster-role-template.yaml](cluster-role-template.yaml): the `gitops-watcher-wake` role, with only `use` on `virtualclusterinstances` and `create` on `virtualclusterinstances/kubeconfig`.
- [user-all-projects.yaml](user-all-projects.yaml): the `gitops-watcher` user with that role in every project, including ones created later.
- [user-selected-projects.yaml](user-selected-projects.yaml): the same user without a role of its own, for limiting it to some projects.
- [project-member.yaml](project-member.yaml): the `spec.members` entry to add to each selected project. It is a fragment, not a standalone Project.

## All Projects

Apply both manifests to the cluster that runs vCluster Platform, with a kubeconfig that can reach the Platform management API:

```bash
kubectl apply -f cluster-role-template.yaml
kubectl apply -f user-all-projects.yaml
```

## Selected Projects

```bash
kubectl apply -f cluster-role-template.yaml
kubectl apply -f user-selected-projects.yaml
```

Then add the member from [project-member.yaml](project-member.yaml) to each project, without removing its existing members, for example in the project's member settings in the Platform UI, or with `kubectl edit projects.management.loft.sh <project>`.

## Use the User

With the Helm chart:

```bash
--set watcher.wake.accessKey.user=gitops-watcher
```

Or set `WATCH_WAKE_ACCESS_KEY_USER=gitops-watcher` on the watcher Deployment. The watcher then impersonates `gitops-watcher` to get a short-lived wake token per tenant cluster, which needs the impersonation rules in the chart or in [deploy/watcher-rbac.yaml](../../deploy/watcher-rbac.yaml).

Its wake activity shows as `loft:user:gitops-watcher` in each VCI's `sleepmode.loft.sh/last-activity-info`.
