{{/* Expand the chart name. */}}
{{- define "vcluster-gitops-watcher.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Create a release-scoped name. */}}
{{- define "vcluster-gitops-watcher.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/* Release-scoped name for the optional wakeup proxy. */}}
{{- define "vcluster-gitops-watcher.proxyFullname" -}}
{{- printf "%s-proxy" (include "vcluster-gitops-watcher.fullname" . | trunc 57 | trimSuffix "-") }}
{{- end }}

{{/* Chart label. */}}
{{- define "vcluster-gitops-watcher.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Common labels. */}}
{{- define "vcluster-gitops-watcher.labels" -}}
helm.sh/chart: {{ include "vcluster-gitops-watcher.chart" . }}
app.kubernetes.io/name: {{ include "vcluster-gitops-watcher.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: vcluster-gitops-watcher
{{- end }}

{{/* Immutable watcher selector labels. */}}
{{- define "vcluster-gitops-watcher.watcherSelectorLabels" -}}
app.kubernetes.io/name: {{ include "vcluster-gitops-watcher.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: watcher
{{- end }}

{{/* Immutable proxy selector labels. */}}
{{- define "vcluster-gitops-watcher.proxySelectorLabels" -}}
app.kubernetes.io/name: {{ include "vcluster-gitops-watcher.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: proxy
{{- end }}

{{/* Watcher ServiceAccount name. */}}
{{- define "vcluster-gitops-watcher.watcherServiceAccountName" -}}
{{- if .Values.watcher.serviceAccount.create }}
{{- default (include "vcluster-gitops-watcher.fullname" .) .Values.watcher.serviceAccount.name }}
{{- else }}
{{- required "watcher.serviceAccount.name is required when watcher.serviceAccount.create is false" .Values.watcher.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* Proxy ServiceAccount name. */}}
{{- define "vcluster-gitops-watcher.proxyServiceAccountName" -}}
{{- if .Values.proxy.serviceAccount.create }}
{{- default (include "vcluster-gitops-watcher.proxyFullname" .) .Values.proxy.serviceAccount.name }}
{{- else }}
{{- required "proxy.serviceAccount.name is required when proxy.serviceAccount.create is false" .Values.proxy.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* Image reference; digest takes precedence over tag. Pass (dict "image" .Values.x.image "root" $). */}}
{{- define "vcluster-gitops-watcher.image" -}}
{{- if .image.digest -}}
{{- printf "%s@%s" .image.repository .image.digest -}}
{{- else -}}
{{- printf "%s:%s" .image.repository (.image.tag | default .root.Chart.AppVersion) -}}
{{- end -}}
{{- end }}

{{/* Argo CD Application namespace. */}}
{{- define "vcluster-gitops-watcher.applicationNamespace" -}}
{{- default .Values.argocd.namespace .Values.argocd.applicationNamespace }}
{{- end }}

{{/* Argo CD cluster Secret namespace. */}}
{{- define "vcluster-gitops-watcher.clusterSecretNamespace" -}}
{{- default .Values.argocd.namespace .Values.argocd.clusterSecretNamespace }}
{{- end }}

{{/* Effective wake upstream for the watcher; empty disables wake requests. */}}
{{- define "vcluster-gitops-watcher.wakeUpstreamBase" -}}
{{- if and .Values.watcher.wake.useProxy .Values.proxy.enabled -}}
{{- printf "http://%s.%s.svc:%v" (include "vcluster-gitops-watcher.proxyFullname" .) .Release.Namespace .Values.proxy.service.port -}}
{{- else -}}
{{- .Values.watcher.wake.upstreamBase -}}
{{- end -}}
{{- end }}
