package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
)

// manageAnnotationName, under the annotation prefix, opts one
// VirtualClusterInstance out of the watcher ("false") or back in ("true").
const manageAnnotationName = "manage"

// manageAnnotation is the full key of the per-VCI manage override.
func (cfg *watcherConfig) manageAnnotation() string {
	prefix := cfg.annotationPrefix
	if prefix == "" {
		prefix = defaultAnnotationPrefix
	}
	return prefix + "/" + manageAnnotationName
}

// vciUnmanagedReason reports why the watcher must leave a VCI alone, or "" when
// it manages it. A tenant cluster with private nodes cannot sleep, so pausing
// Argo CD for it saves nothing and can only freeze work that Argo CD has to
// finish by reconciling. The manage annotation overrides the detection either
// way.
func vciUnmanagedReason(cfg *watcherConfig, vci virtualClusterInstance) string {
	switch strings.ToLower(strings.TrimSpace(vci.Metadata.Annotations[cfg.manageAnnotation()])) {
	case "false":
		return cfg.manageAnnotation() + "=false"
	case "true":
		return ""
	}
	switch {
	case vci.Spec.Standalone || vciValuesBool(vci, "controlPlane", "standalone", "enabled"):
		return "standalone, which cannot sleep"
	case vciUsesPrivateNodes(vci):
		return "private nodes (privateNodes.enabled: true), which cannot sleep"
	case (vci.Spec.External || vci.Metadata.Annotations[skipHelmDeployAnnotation] == "true") && vci.Spec.ClusterRef == (vciClusterRef{}):
		return "external and not connected to a cluster, so Platform sleep mode does not apply"
	case strings.TrimSpace(vci.Metadata.Annotations[sleepScopeAnnotation]) == "workloads-only":
		return sleepScopeAnnotation + "=workloads-only: only its workloads sleep, so the VCI never looks asleep"
	}
	return ""
}

const (
	skipHelmDeployAnnotation = "loft.sh/skip-helm-deploy"
	sleepScopeAnnotation     = "sleepmode.loft.sh/scope"
)

// vciUsesPrivateNodes reads privateNodes.enabled from the VCI's vcluster.yaml.
// For a templated VCI only status.virtualCluster carries the resolved values;
// spec.template covers a VCI whose status has not been written yet.
func vciUsesPrivateNodes(vci virtualClusterInstance) bool {
	return vciValuesBool(vci, "privateNodes", "enabled")
}

// vciValuesBool reads a boolean at path from the VCI's vcluster.yaml: the
// resolved values in status.virtualCluster, else spec.template.
func vciValuesBool(vci virtualClusterInstance, path ...string) bool {
	if vci.Status.VirtualCluster != nil && yamlBoolAt(vci.Status.VirtualCluster.HelmRelease.Values, path...) {
		return true
	}
	return vci.Spec.Template != nil && yamlBoolAt(vci.Spec.Template.HelmRelease.Values, path...)
}

// privateNodesEnabled reports whether a vcluster.yaml document sets
// privateNodes.enabled to true.
func privateNodesEnabled(values string) bool {
	return yamlBoolAt(values, "privateNodes", "enabled")
}

// yamlBoolAt reports whether the vcluster.yaml document has true at the key
// path, in block style or with a flow mapping at the last level. It reads only
// what the watcher needs, so the build stays standard-library-only. Anything it
// cannot read counts as false, which leaves the VCI managed as before.
func yamlBoolAt(document string, path ...string) bool {
	if len(path) == 0 {
		return false
	}
	lines := strings.Split(document, "\n")
	for i := 0; i < len(lines); i++ {
		line := strings.TrimRight(stripYAMLComment(lines[i]), " \t\r")
		if indentation(line) != 0 {
			continue
		}
		rest, ok := strings.CutPrefix(line, path[0]+":")
		if !ok {
			continue
		}
		rest = strings.TrimSpace(rest)
		if len(path) == 1 {
			return yamlTrue(rest)
		}
		if strings.HasPrefix(rest, "{") {
			return len(path) == 2 && flowMappingBool(rest, path[1])
		}
		if rest != "" {
			return false
		}
		// Collect the nested block, dedented, and look one level deeper.
		var block []string
		childIndent := -1
		for j := i + 1; j < len(lines); j++ {
			child := strings.TrimRight(stripYAMLComment(lines[j]), " \t\r")
			if strings.TrimSpace(child) == "" {
				continue
			}
			ind := indentation(child)
			if ind == 0 {
				break
			}
			if childIndent < 0 {
				childIndent = ind
			}
			if ind < childIndent {
				break
			}
			block = append(block, child[childIndent:])
		}
		return yamlBoolAt(strings.Join(block, "\n"), path[1:]...)
	}
	return false
}

func flowMappingBool(flow, key string) bool {
	flow = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(flow), "{"), "}")
	for _, pair := range strings.Split(flow, ",") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(pair), key+":"); ok {
			return yamlTrue(value)
		}
	}
	return false
}

func yamlTrue(value string) bool {
	switch strings.TrimSpace(value) {
	case "true", "True", "TRUE":
		return true
	}
	return false
}

func indentation(line string) int {
	return len(line) - len(strings.TrimLeft(line, " \t"))
}

// stripYAMLComment drops a trailing comment. vcluster.yaml keys and the values
// read here never contain '#', so quoting is not tracked.
func stripYAMLComment(line string) string {
	if i := strings.Index(line, "#"); i == 0 || (i > 0 && (line[i-1] == ' ' || line[i-1] == '\t')) {
		return line[:i]
	}
	return line
}

// safeConcatNameMax mirrors the platform's encoding.SafeConcatNameMax: a name
// longer than max keeps its first max-8 characters, then "-" and the first 7
// hex characters of the SHA-256 of the full name.
func safeConcatNameMax(name string, max int) string {
	if len(name) <= max {
		return name
	}
	digest := sha256.Sum256([]byte(name))
	return name[:max-8] + "-" + hex.EncodeToString(digest[:])[:7]
}

// leaveUnmanagedVCI is the whole reconcile for a VCI the watcher does not
// manage: no pause, no health patching, no wake, no sleep. A skip-reconcile an
// earlier watcher version left on its Secret would keep Argo CD frozen for
// good, so it is removed, once.
func leaveUnmanagedVCI(ctx context.Context, cfg *watcherConfig, runtime *watcherRuntime, vci virtualClusterInstance, runtimeKey, reason string, paused bool, secretMetaName string) error {
	if paused {
		if err := cfg.api.patchSecretSkipReconcile(ctx, cfg.argocdClusterSecretNamespace, secretMetaName, false); err != nil {
			return fmt.Errorf("resume cluster secret %s/%s of unmanaged VCI %s/%s: %w", cfg.argocdClusterSecretNamespace, secretMetaName, vci.Metadata.Namespace, vci.Metadata.Name, err)
		}
		log.Printf("removed %s from cluster secret %s/%s: VCI %s/%s is not managed (%s)", argocdSkipReconcileAnnotation, cfg.argocdClusterSecretNamespace, secretMetaName, vci.Metadata.Namespace, vci.Metadata.Name, reason)
	}
	if runtime.unmanagedLogged[runtimeKey] != reason {
		runtime.unmanagedLogged[runtimeKey] = reason
		log.Printf("not managing VCI %s/%s: %s", vci.Metadata.Namespace, vci.Metadata.Name, reason)
	}
	delete(runtime.readyUnpausedAt, runtimeKey)
	delete(runtime.lastWakeAttempt, runtimeKey)
	delete(runtime.sleepAfterSync, runtimeKey)
	return nil
}
