{{/* Base name, truncated to the 63-char DNS limit. */}}
{{- define "growlerdb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully-qualified release name. */}}
{{- define "growlerdb.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "growlerdb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Common labels on every object. */}}
{{- define "growlerdb.labels" -}}
helm.sh/chart: {{ include "growlerdb.chart" . }}
{{ include "growlerdb.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: growlerdb
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "growlerdb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "growlerdb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Per-component names + selector labels (component = control-plane | node | gateway). */}}
{{- define "growlerdb.controlplane.fullname" -}}{{ include "growlerdb.fullname" . }}-controlplane{{- end -}}
{{- define "growlerdb.node.fullname" -}}{{ include "growlerdb.fullname" . }}-node{{- end -}}
{{- define "growlerdb.gateway.fullname" -}}{{ include "growlerdb.fullname" . }}-gateway{{- end -}}

{{- define "growlerdb.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "growlerdb.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "growlerdb.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/* The Secret name holding object-store/catalog creds (existing or chart-managed). */}}
{{- define "growlerdb.secretName" -}}
{{- if .Values.credentials.existingSecret -}}
{{- .Values.credentials.existingSecret -}}
{{- else -}}
{{- include "growlerdb.fullname" . }}-creds
{{- end -}}
{{- end -}}

{{/* The control-plane's in-cluster gRPC endpoint, used by nodes (--register) and the gateway. */}}
{{- define "growlerdb.controlplane.endpoint" -}}
{{- printf "http://%s:%d" (include "growlerdb.controlplane.fullname" .) (int .Values.controlPlane.grpcPort) -}}
{{- end -}}

{{/* Iceberg env: non-secret config from the ConfigMap + credentials from the Secret. Used by
     every component (control-plane resolves sources; nodes read/hydrate; gateway is uniform). */}}
{{- define "growlerdb.icebergEnv" -}}
- name: GROWLERDB_CATALOG_URI
  valueFrom: { configMapKeyRef: { name: {{ include "growlerdb.fullname" . }}-config, key: catalogUri } }
- name: GROWLERDB_WAREHOUSE
  valueFrom: { configMapKeyRef: { name: {{ include "growlerdb.fullname" . }}-config, key: warehouse } }
- name: GROWLERDB_S3_ENDPOINT
  valueFrom: { configMapKeyRef: { name: {{ include "growlerdb.fullname" . }}-config, key: s3Endpoint } }
- name: GROWLERDB_S3_REGION
  valueFrom: { configMapKeyRef: { name: {{ include "growlerdb.fullname" . }}-config, key: s3Region } }
{{- if .Values.iceberg.catalogScope }}
- name: GROWLERDB_CATALOG_SCOPE
  valueFrom: { configMapKeyRef: { name: {{ include "growlerdb.fullname" . }}-config, key: catalogScope } }
{{- end }}
- name: GROWLERDB_CATALOG_CREDENTIAL
  valueFrom: { secretKeyRef: { name: {{ include "growlerdb.secretName" . }}, key: catalogCredential } }
- name: GROWLERDB_S3_ACCESS_KEY
  valueFrom: { secretKeyRef: { name: {{ include "growlerdb.secretName" . }}, key: s3AccessKey } }
- name: GROWLERDB_S3_SECRET_KEY
  valueFrom: { secretKeyRef: { name: {{ include "growlerdb.secretName" . }}, key: s3SecretKey } }
{{- end -}}

{{/* Observability env shared by every component: export OTLP traces when an endpoint is
     configured. Metrics/health are served on each component's metrics port regardless. */}}
{{- define "growlerdb.observabilityEnv" -}}
{{- with .Values.observability.otlpEndpoint }}
- name: GROWLERDB_OTLP_ENDPOINT
  value: {{ . | quote }}
{{- end }}
{{- end -}}

{{/* Standard liveness/readiness probes against a component's telemetry port. */}}
{{- define "growlerdb.probes" -}}
livenessProbe:
  httpGet: { path: /healthz, port: metrics }
  initialDelaySeconds: 10
  periodSeconds: 15
readinessProbe:
  httpGet: { path: /readyz, port: metrics }
  initialDelaySeconds: 5
  periodSeconds: 5
  failureThreshold: 30
{{- end -}}
