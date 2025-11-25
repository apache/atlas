# Logstash Helm Chart

[![Build Status](https://img.shields.io/jenkins/s/https/devops-ci.elastic.co/job/elastic+helm-charts+main.svg)](https://devops-ci.elastic.co/job/elastic+helm-charts+main/) [![Artifact HUB](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/elastic)](https://artifacthub.io/packages/search?repo=elastic)

This Helm chart is a lightweight way to configure and run our official
[Logstash Docker image][].

> **Warning**
> When it comes to running the Elastic on Kubernetes infrastructure, we
> recommend [Elastic Cloud on Kubernetes][] (ECK) as the best way to run and manage
> the Elastic Stack.
>
> ECK offers many operational benefits for both our basic-tier and our
> enterprise-tier customers, such as spinning up cluster nodes that were lost on
> failed infrastructure, seamless upgrades, rolling cluster changes, and much
> much more.
>
> With the release of the Elastic Stack Helm charts for Elastic version 8.5.1,
> we are handing over the ongoing maintenance of our Elastic Stack Helm charts
> to the community and contributors. This repository will finally be archived
> after 6 months time. Elastic Stacks deployed on Kubernetes through Helm charts
> will still be fully supported under EOL limitations.
>
> Since we want to provide an even better experience for our customers by
> running the Elastic Stack on Kubernetes, we will continue maintaining the
> Helm charts applicable to ECK Custom Resources. These charts can be found in
> the [ECK repository][eck-charts].
>
> Helm charts will currently be maintained for ECK Enterprise-tier customers,
> however, we encourage the community to engage with the existing Helm charts
> for the Elastic Stack and continue supporting their ongoing maintenance.
>
> See <https://github.com/elastic/helm-charts/issues/1731> for more details.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Requirements](#requirements)
- [Installing](#installing)
  - [Install a released version using the Helm repository](#install-a-released-version-using-the-helm-repository)
  - [Install a development version using the main branch](#install-a-development-version-using-the-main-branch)
- [Upgrading](#upgrading)
- [Usage notes](#usage-notes)
- [Configuration](#configuration)
- [FAQ](#faq)
  - [How to install OSS version of Logstash?](#how-to-install-oss-version-of-logstash)
  - [How to install plugins?](#how-to-install-plugins)
- [Contributing](#contributing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->
<!-- Use this to update TOC: -->
<!-- docker run --entrypoint doctoc --rm -it -v $(pwd):/usr/src jorgeandrada/doctoc README.md --github --no-title -->


## Requirements

See [supported configurations][] for more details.

## Installing

### Install a released version using the Helm repository

* Add the Elastic Helm charts repo:
`helm repo add elastic https://helm.elastic.co`

* Install it: `helm install logstash elastic/logstash`

### Install a development version using the main branch

* Clone the git repo: `git clone git@github.com:elastic/helm-charts.git`

* Install it: `helm install logstash ./helm-charts/logstash --set imageTag=8.5.1`


## Upgrading

Please always check [CHANGELOG.md][] and [BREAKING_CHANGES.md][] before
upgrading to a new chart version.


## Usage notes

* This repo includes several [examples][] of configurations that can be used
as a reference. They are also used in the automated testing of this chart
* Automated testing of this chart is currently only run against GKE (Google
Kubernetes Engine).
* The chart deploys a StatefulSet and by default will do an automated rolling
update of your cluster. It does this by waiting for the cluster health to become
green after each instance is updated. If you prefer to update manually you can
set `OnDelete` [updateStrategy][].
* It is important to verify that the JVM heap size in `logstashJavaOpts` and to
set the CPU/Memory `resources` to something suitable for your cluster.
* We have designed this chart to be very un-opinionated about how to configure
Logstash. It exposes ways to set environment variables and mount secrets inside
of the container. Doing this makes it much easier for this chart to support
multiple versions with minimal changes.
* `logstash.yml` configuration files can be set either by a ConfigMap using
`logstashConfig` in `values.yml` or by environment variables using `extraEnvs`
in `values.yml` , however Logstash Docker image can't mix both methods as
defining settings with environment variables causes `logstash.yml` to be
modified in place while using ConfigMap bind-mount the same file (more details
in this [note][]).
* When overriding `logstash.yml`, `http.host: 0.0.0.0` should always be included
to make default probes work. If restricting HTTP API to 127.0.0.1 is required by
using `http.host: 127.0.0.1`, default probes should be disabled or overridden
(see [values.yaml][] for the good syntax).
* An ingress is provided that can be used to expose the HTTP port. This can be
useful for the [http input plugin][], for instance.


## Configuration

| Parameter                 | Description                                                                                                                                                                                                                          | Default                               |
|---------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------|
| `antiAffinityTopologyKey` | The [anti-affinity][] topology key]. By default this will prevent multiple Logstash nodes from running on the same Kubernetes node                                                                                                   | `kubernetes.io/hostname`              |
| `antiAffinity`            | Setting this to hard enforces the [anti-affinity][] rules. If it is set to soft it will be done "best effort". Other values will be ignored                                                                                          | `hard`                                |
| `envFrom`                 | Templatable string to be passed to the [environment from variables][] which will be appended to the `envFrom:` definition for the container                                                                                          | `[]`                                  |
| `extraContainers`         | Templatable string of additional containers to be passed to the `tpl` function                                                                                                                                                       | `[]`                                  |
| `extraEnvs`               | Extra [environment variables][] which will be appended to the `env:` definition for the container                                                                                                                                    | `[]`                                  |
| `extraInitContainers`     | Templatable string of additional `initContainers` to be passed to the `tpl` function                                                                                                                                                 | `[]`                                  |
| `extraPorts`              | An array of extra ports to open on the pod                                                                                                                                                                                           | `[]`                                  |
| `extraVolumeMounts`       | Templatable string of additional `volumeMounts` to be passed to the `tpl` function                                                                                                                                                   | `[]`                                  |
| `extraVolumes`            | Templatable string of additional `volumes` to be passed to the `tpl` function                                                                                                                                                        | `[]`                                  |
| `fullnameOverride`        | Overrides the full name of the resources. If not set the name will default to " `.Release.Name` - `.Values.nameOverride or .Chart.Name` "                                                                                            | `""`                                  |
| `hostAliases`             | Configurable [hostAliases][]                                                                                                                                                                                                         | `[]`                                  |
| `httpPort`                | The http port that Kubernetes will use for the healthchecks and the service                                                                                                                                                          | `9600`                                |
| `imagePullPolicy`         | The Kubernetes [imagePullPolicy][] value                                                                                                                                                                                             | `IfNotPresent`                        |
| `imagePullSecrets`        | Configuration for [imagePullSecrets][] so that you can use a private registry for your image                                                                                                                                         | `[]`                                  |
| `imageTag`                | The Logstash Docker image tag                                                                                                                                                                                                        | `8.5.1`                               |
| `image`                   | The Logstash Docker image                                                                                                                                                                                                            | `docker.elastic.co/logstash/logstash` |
| `labels`                  | Configurable [labels][] applied to all Logstash pods                                                                                                                                                                                 | `{}`                                  |
| `ingress`                 | Configurable [ingress][] for external access to Logstash HTTP port.                                                                                                                                                                  | see [values.yaml][]                   |
| `lifecycle`               | Allows you to add lifecycle configuration. See [values.yaml][] for an example of the formatting                                                                                                                                      | `{}`                                  |
| `livenessProbe`           | Configuration fields for the liveness [probe][]                                                                                                                                                                                      | see [values.yaml][]                   |
| `logstashConfig`          | Allows you to add any config files in `/usr/share/logstash/config/` such as `logstash.yml` and `log4j2.properties` See [values.yaml][] for an example of the formatting                                                              | `{}`                                  |
| `logstashJavaOpts`        | Java options for Logstash. This is where you should configure the JVM heap size                                                                                                                                                      | `-Xmx1g -Xms1g`                       |
| `logstashPipeline`        | Allows you to add any pipeline files in `/usr/share/logstash/pipeline/`                                                                                                                                                              | `{}`                                  |
| `logstashPatternDir`      | Allows you to define a custom directory to store pattern files                                                                                                                                                                       | `/usr/share/logstash/patterns/`       |
| `logstashPattern`         | Allows you to add any pattern files in `logstashPatternDir`                                                                                                                                                                          | `{}`                                  |
| `maxUnavailable`          | The [maxUnavailable][] value for the pod disruption budget. By default this will prevent Kubernetes from having more than 1 unhealthy pod in the node group                                                                          | `1`                                   |
| `nameOverride`            | Overrides the chart name for resources. If not set the name will default to `.Chart.Name`                                                                                                                                            | `""`                                  |
| `nodeAffinity`            | Value for the [node affinity settings][]                                                                                                                                                                                             | `{}`                                  |
| `podAffinity`             | Value for the [pod affinity settings][]                                                                                                                                                                                              | `{}`                                  |
| `nodeSelector`            | Configurable [nodeSelector][] so that you can target specific nodes for your Logstash cluster                                                                                                                                        | `{}`                                  |
| `persistence`             | Enables a persistent volume for Logstash data                                                                                                                                                                                        | see [values.yaml][]                   |
| `podAnnotations`          | Configurable [annotations][] applied to all Logstash pods                                                                                                                                                                            | `{}`                                  |
| `podManagementPolicy`     | By default Kubernetes [deploys StatefulSets serially][]. This deploys them in parallel so that they can discover each other                                                                                                          | `Parallel`                            |
| `podSecurityContext`      | Allows you to set the [securityContext][] for the pod                                                                                                                                                                                | see [values.yaml][]                   |
| `podSecurityPolicy`       | Configuration for create a pod security policy with minimal permissions to run this Helm chart with `create: true` Also can be used to reference an external pod security policy with `name: "externalPodSecurityPolicy"`            | see [values.yaml][]                   |
| `priorityClassName`       | The name of the [PriorityClass][]. No default is supplied as the PriorityClass must be created first                                                                                                                                 | `""`                                  |
| `rbac`                    | Configuration for creating a role, role binding and service account as part of this Helm chart with `create: true` Also can be used to reference an external service account with `serviceAccountName: "externalServiceAccountName"` | see [values.yaml][]                   |
| `readinessProbe`          | Configuration fields for the readiness [probe][]                                                                                                                                                                                     | see [values.yaml][]                   |
| `replicas`                | Kubernetes replica count for the StatefulSet (i.e. how many pods)                                                                                                                                                                    | `1`                                   |
| `resources`               | Allows you to set the [resources][] for the StatefulSet                                                                                                                                                                              | see [values.yaml][]                   |
| `schedulerName`           | Name of the [alternate scheduler][]                                                                                                                                                                                                  | `""`                                  |
| `secrets`                 | Allows you easily create a secret from as variables or file. For add secrets from file, add suffix `.filepath` to the key of secret key. The value will be encoded to base64. Useful for store certificates and other secrets.       | See [values.yaml][]                   |
| `secretMounts`            | Allows you easily mount a secret as a file inside the StatefulSet. Useful for mounting certificates and other secrets. See [values.yaml][] for an example                                                                            | `[]`                                  |
| `securityContext`         | Allows you to set the [securityContext][] for the container                                                                                                                                                                          | see [values.yaml][]                   |
| `service`                 | Configurable [service][] to expose the Logstash service.                                                                                                                                                                             | see [values.yaml][]                   |
| `terminationGracePeriod`  | The [terminationGracePeriod][] in seconds used when trying to stop the pod                                                                                                                                                           | `120`                                 |
| `tolerations`             | Configurable [tolerations][]                                                                                                                                                                                                         | `[]`                                  |
| `updateStrategy`          | The [updateStrategy][] for the StatefulSet. By default Kubernetes will wait for the cluster to be green after upgrading each pod. Setting this to `OnDelete` will allow you to manually delete each pod during upgrades              | `RollingUpdate`                       |
| `volumeClaimTemplate`     | Configuration for the [volumeClaimTemplate for StatefulSets][]. You will want to adjust the storage (default `30Gi` ) and the `storageClassName` if you are using a different storage class                                          | see [values.yaml][]                   |


## FAQ

### How to install OSS version of Logstash?

Deploying OSS version of Logstash can be done by setting `image` value to
[Logstash OSS Docker image][]

An example of Logstash deployment using OSS version can be found in
[examples/oss][].

### How to install plugins?

The recommended way to install plugins into our Docker images is to create a
[custom Docker image][].

The Dockerfile would look something like:

```
ARG logstash_version
FROM docker.elastic.co/logstash/logstash:${logstash_version}
RUN bin/logstash-plugin install logstash-output-kafka
```

And then updating the `image` in values to point to your custom image.

There are a couple reasons we recommend this:

1. Tying the availability of Logstash to the download service to install plugins
is not a great idea or something that we recommend. Especially in Kubernetes
where it is normal and expected for a container to be moved to another host at
random times.
2. Mutating the state of a running Docker image (by installing plugins) goes
against best practices of containers and immutable infrastructure.


## Contributing

Please check [CONTRIBUTING.md][] before any contribution or for any questions
about our development and testing process.

[alternate scheduler]: https://kubernetes.io/docs/tasks/administer-cluster/configure-multiple-schedulers/#specify-schedulers-for-pods
[annotations]: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/
[anti-affinity]: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity
[BREAKING_CHANGES.md]: https://github.com/elastic/helm-charts/blob/main/BREAKING_CHANGES.md
[CHANGELOG.md]: https://github.com/elastic/helm-charts/blob/main/CHANGELOG.md
[CONTRIBUTING.md]: https://github.com/elastic/helm-charts/blob/main/CONTRIBUTING.md
[custom docker image]: https://www.elastic.co/guide/en/logstash/current/docker-config.html#_custom_images
[deploys statefulsets serially]: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#pod-management-policies
[eck-charts]: https://github.com/elastic/cloud-on-k8s/tree/master/deploy
[elastic cloud on kubernetes]: https://github.com/elastic/cloud-on-k8s
[environment from variables]: https://kubernetes.io/docs/tasks/configure-pod-container/configure-pod-configmap/#configure-all-key-value-pairs-in-a-configmap-as-container-environment-variables
[environment variables]: https://kubernetes.io/docs/tasks/inject-data-application/define-environment-variable-container/#using-environment-variables-inside-of-your-config
[examples]: https://github.com/elastic/helm-charts/tree/main/logstash/examples
[examples/oss]: https://github.com/elastic/helm-charts/tree/main/logstash/examples/oss
[hostAliases]: https://kubernetes.io/docs/concepts/services-networking/add-entries-to-pod-etc-hosts-with-host-aliases/
[http input plugin]: https://www.elastic.co/guide/en/logstash/current/plugins-inputs-http.html
[imagePullPolicy]: https://kubernetes.io/docs/concepts/containers/images/#updating-images
[imagePullSecrets]: https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/#create-a-pod-that-uses-your-secret
[ingress]: https://kubernetes.io/docs/concepts/services-networking/ingress/
[labels]: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
[logstash docker image]: https://www.elastic.co/guide/en/logstash/current/docker.html
[logstash oss docker image]: https://www.docker.elastic.co/r/logstash/logstash-oss
[maxUnavailable]: https://kubernetes.io/docs/tasks/run-application/configure-pdb/#specifying-a-poddisruptionbudget
[node affinity settings]: https://kubernetes.io/docs/tasks/configure-pod-container/assign-pods-nodes-using-node-affinity/
[nodeSelector]: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#nodeselector
[note]: https://www.elastic.co/guide/en/logstash/current/docker-config.html#docker-env-config
[pod affinity settings]: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#inter-pod-affinity-and-anti-affinity
[priorityClass]: https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/#priorityclass
[probe]: https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-probes/
[resources]: https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/
[securityContext]: https://kubernetes.io/docs/tasks/configure-pod-container/security-context/#set-the-security-context-for-a-pod
[service]: https://kubernetes.io/docs/concepts/services-networking/service/
[supported configurations]: https://github.com/elastic/helm-charts/tree/main/README.md#supported-configurations
[terminationGracePeriod]: https://kubernetes.io/docs/concepts/workloads/pods/pod/#termination-of-pods
[tolerations]: https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/
[updateStrategy]: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/
[values.yaml]: https://github.com/elastic/helm-charts/tree/main/logstash/values.yaml
[volumeClaimTemplate for statefulsets]: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#stable-storage
