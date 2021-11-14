{{/*
Expand the name of the chart.
*/}}
{{- define "wasm-fybrik-module.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "wasm-fybrik-module.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "wasm-fybrik-module.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "wasm-fybrik-module.labels" -}}
helm.sh/chart: {{ include "wasm-fybrik-module.chart" . }}
{{ include "wasm-fybrik-module.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "wasm-fybrik-module.selectorLabels" -}}
app.kubernetes.io/name: {{ include "wasm-fybrik-module.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "wasm-fybrik-module.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "wasm-fybrik-module.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}


{{- define "wasm-fybrik-module-chart.configtemplate" }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "wasm-fybrik-module.fullname" . }}
data:
  conf.yaml: |- 
{{- if .Values.config_override }}
{{ .Values.config_override  | indent 4}}
{{- else }}
{{ tpl ( .Files.Get "files/conf.yaml" ) . | indent 4 }}
{{- end -}}
{{- end }}