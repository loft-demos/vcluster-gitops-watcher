package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	// vCluster Platform asks its ArgoCDApplication controller to refresh or sync
	// an Argo CD Application with these annotations on the ArgoCDApplication
	// (Stack task retries, the Platform UI's Refresh and Sync buttons). It removes
	// them only once Argo CD has done it, so while one is present the Platform is
	// waiting on Argo CD.
	platformArgoCDRefreshAnnotation = "argocdapplication.loft.sh/refresh"
	platformArgoCDSyncAnnotation    = "argocdapplication.loft.sh/sync"

	platformArgoCDApplicationsPath = "/apis/management.loft.sh/v1/argocdapplications"
	// platformArgoCDApplicationsRecheck bounds how long a missing permission or
	// API keeps the watcher from looking again.
	platformArgoCDApplicationsRecheck = 5 * time.Minute
)

type platformArgoCDDestinationVirtualCluster struct {
	Name string `json:"name"`
}

type platformArgoCDDestination struct {
	VirtualCluster *platformArgoCDDestinationVirtualCluster `json:"virtualCluster,omitempty"`
}

type platformArgoCDApplicationSpec struct {
	Destination platformArgoCDDestination `json:"destination"`
}

// platformArgoCDApplication is the part of a management.loft.sh/v1
// ArgoCDApplication the watcher reads.
type platformArgoCDApplication struct {
	Metadata metadata                      `json:"metadata"`
	Spec     platformArgoCDApplicationSpec `json:"spec"`
}

func (a *kubernetesAPI) listPlatformArgoCDApplications(ctx context.Context) ([]platformArgoCDApplication, error) {
	return listKubernetesResources[platformArgoCDApplication](ctx, a, platformArgoCDApplicationsPath, nil)
}

// listPlatformArgoCDApplicationsOptional lists the Platform's ArgoCDApplications,
// treating a missing API or permission as "none" and checking again later.
func listPlatformArgoCDApplicationsOptional(ctx context.Context, cfg *watcherConfig, runtime *watcherRuntime) []platformArgoCDApplication {
	if runtime != nil && time.Now().Before(runtime.platformAppsRecheckAt) {
		return nil
	}
	items, err := cfg.api.listPlatformArgoCDApplications(ctx)
	if err != nil {
		var statusErr *apiStatusError
		if errors.As(err, &statusErr) {
			switch statusErr.StatusCode {
			case http.StatusNotFound, http.StatusForbidden, http.StatusMethodNotAllowed:
				if runtime != nil {
					runtime.platformAppsRecheckAt = time.Now().Add(platformArgoCDApplicationsRecheck)
					if !runtime.platformAppsUnavailableLogged {
						runtime.platformAppsUnavailableLogged = true
						log.Printf("vCluster Platform ArgoCDApplications not readable (%s); Platform refresh and sync requests are not tracked. Grant list on management.loft.sh argocdapplications", statusErr.Status)
					}
				}
				return nil
			}
		}
		log.Printf("list vCluster Platform ArgoCDApplications failed; continuing without them this pass: %v", err)
		return nil
	}
	if runtime != nil && runtime.platformAppsUnavailableLogged {
		runtime.platformAppsUnavailableLogged = false
		log.Printf("vCluster Platform ArgoCDApplications are readable again; tracking Platform refresh and sync requests")
	}
	return items
}

// platformTriggersByVCI maps a VCI (namespace/name) to its ArgoCDApplications
// that carry a pending refresh or sync, keyed by name with a fingerprint of the
// request.
func platformTriggersByVCI(items []platformArgoCDApplication) map[string]map[string]string {
	out := map[string]map[string]string{}
	for _, item := range items {
		vc := item.Spec.Destination.VirtualCluster
		if vc == nil || strings.TrimSpace(vc.Name) == "" {
			continue
		}
		refresh := strings.TrimSpace(item.Metadata.Annotations[platformArgoCDRefreshAnnotation])
		sync := strings.TrimSpace(item.Metadata.Annotations[platformArgoCDSyncAnnotation])
		if refresh == "" && sync == "" {
			continue
		}
		key := instanceKey(item.Metadata.Namespace, vc.Name)
		if key == "" {
			continue
		}
		if out[key] == nil {
			out[key] = map[string]string{}
		}
		out[key]["argocdapplication/"+item.Metadata.Name] = "refresh=" + refresh + ";sync=" + sync
	}
	return out
}

// pendingPlatformWork is what vCluster Platform is waiting on Argo CD for in this
// VCI: refresh and sync requests on its ArgoCDApplications, and Argo CD
// Applications it created that Argo CD has never reconciled (a Stack deployed
// while the cluster slept). Keys name the object, values fingerprint the request.
func pendingPlatformWork(triggers map[string]string, apps []application) map[string]string {
	work := map[string]string{}
	for name, fingerprint := range triggers {
		work[name] = fingerprint
	}
	for _, app := range apps {
		if applicationIsPlatformManaged(app) && applicationAwaitsFirstReconcile(app) {
			work["application/"+app.Metadata.Name] = "new"
		}
	}
	return work
}

// newPlatformWork is the pending work this watcher has not acted on yet.
func newPlatformWork(runtime *watcherRuntime, runtimeKey string, work map[string]string) []string {
	var out []string
	seen := runtime.observedPlatformWork[runtimeKey]
	for name, fingerprint := range work {
		if seen[name] != fingerprint {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// rememberPlatformWork records the current pending work as acted on. Replacing
// the whole set also forgets requests the Platform has since cleared, so a
// repeat of the same request later counts as new.
func rememberPlatformWork(runtime *watcherRuntime, runtimeKey string, work map[string]string) {
	if len(work) == 0 {
		delete(runtime.observedPlatformWork, runtimeKey)
		return
	}
	copied := make(map[string]string, len(work))
	for name, fingerprint := range work {
		copied[name] = fingerprint
	}
	runtime.observedPlatformWork[runtimeKey] = copied
}

func platformWorkNames(work map[string]string) []string {
	names := make([]string, 0, len(work))
	for name := range work {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
