# Atlas Startup Metrics

This document describes the Prometheus metrics available for monitoring Atlas startup performance. These metrics can be used to build Grafana dashboards to identify startup bottlenecks.

## Overview

All startup metrics are published to the Prometheus endpoint at `/api/atlas/admin/metrics/prometheus`. They use the Micrometer Timer type, which provides:
- Count of invocations
- Total time
- Max time
- Percentiles (p50, p90, p99)

## Metrics Reference

### Service Startup Metrics

| Metric Name | Description | Labels |
|-------------|-------------|--------|
| `atlas_startup_service_duration_seconds` | Time taken to start individual services during Atlas startup | `service` (service class name) |
| `atlas_startup_services_total_duration_seconds` | Total time taken to start all services during Atlas startup | - |

**Source:** `org.apache.atlas.service.Services`

**Example Prometheus queries:**
```promql
# Time taken by each service to start
atlas_startup_service_duration_seconds{quantile="0.5"}

# Total services startup time
atlas_startup_services_total_duration_seconds{quantile="0.5"}

# Identify slowest services
topk(5, atlas_startup_service_duration_seconds{quantile="0.99"})
```

---

### Graph & Index Initialization Metrics

| Metric Name | Description | Labels |
|-------------|-------------|--------|
| `atlas_startup_graph_index_init_duration_seconds` | Time taken to initialize graph indices (Vertex, Edge, Fulltext) | - |
| `atlas_startup_graph_setup_duration_seconds` | Time taken to setup JanusGraph and ElasticSearch index | - |

**Source:** 
- `org.apache.atlas.repository.graph.GraphBackedSearchIndexer`
- `org.apache.atlas.util.RepairIndex`

**Example Prometheus queries:**
```promql
# Graph index initialization time
atlas_startup_graph_index_init_duration_seconds{quantile="0.5"}

# Graph setup time
atlas_startup_graph_setup_duration_seconds{quantile="0.5"}

# Combined graph initialization time
atlas_startup_graph_index_init_duration_seconds{quantile="0.5"} + atlas_startup_graph_setup_duration_seconds{quantile="0.5"}
```

---

### Type Definition Metrics

| Metric Name | Description | Labels |
|-------------|-------------|--------|
| `atlas_startup_typedef_init_duration_seconds` | Time taken to initialize the type definition store | - |
| `atlas_startup_typedef_load_duration_seconds` | Time taken to load bootstrap type definitions from models directory | - |

**Source:** `org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer`

**Example Prometheus queries:**
```promql
# TypeDef initialization time
atlas_startup_typedef_init_duration_seconds{quantile="0.5"}

# Bootstrap typedef loading time
atlas_startup_typedef_load_duration_seconds{quantile="0.5"}

# Total typedef processing time
atlas_startup_typedef_init_duration_seconds{quantile="0.5"} + atlas_startup_typedef_load_duration_seconds{quantile="0.5"}
```

---

### Task Management Metrics

| Metric Name | Description | Labels |
|-------------|-------------|--------|
| `atlas_startup_taskmanager_start_duration_seconds` | Time taken to start the task management system | - |

**Source:** `org.apache.atlas.tasks.TaskFactoryRegistry`

**Example Prometheus queries:**
```promql
# Task manager startup time
atlas_startup_taskmanager_start_duration_seconds{quantile="0.5"}
```

---

### Authorization & Policy Metrics

| Metric Name | Description | Labels |
|-------------|-------------|--------|
| `atlas_startup_roles_load_duration_seconds` | Time taken to load roles from cache/admin during startup | - |
| `atlas_startup_policy_load_duration_seconds` | Time taken to load authorization policies during startup | - |
| `atlas_startup_userstore_load_duration_seconds` | Time taken to load user store (users/groups) during startup | - |

**Source:** `org.apache.atlas.plugin.util.PolicyRefresher`

**Example Prometheus queries:**
```promql
# Roles loading time
atlas_startup_roles_load_duration_seconds{quantile="0.5"}

# Policy loading time
atlas_startup_policy_load_duration_seconds{quantile="0.5"}

# User store loading time
atlas_startup_userstore_load_duration_seconds{quantile="0.5"}

# Total authorization initialization time
atlas_startup_roles_load_duration_seconds{quantile="0.5"} + atlas_startup_policy_load_duration_seconds{quantile="0.5"} + atlas_startup_userstore_load_duration_seconds{quantile="0.5"}
```

---

## Grafana Dashboard Panels

### Recommended Panel Configuration

#### 1. Startup Time Overview (Stat Panel)
```promql
# Total startup time approximation
sum(atlas_startup_services_total_duration_seconds{quantile="0.5"})
```

#### 2. Service Startup Breakdown (Bar Gauge)
```promql
# Per-service startup times
atlas_startup_service_duration_seconds{quantile="0.5"}
```

#### 3. Startup Phase Timeline (Time Series)
```promql
# Graph initialization
atlas_startup_graph_index_init_duration_seconds{quantile="0.5"}

# TypeDef loading
atlas_startup_typedef_load_duration_seconds{quantile="0.5"}

# Policy loading
atlas_startup_policy_load_duration_seconds{quantile="0.5"}
```

#### 4. Authorization Initialization (Pie Chart)
```promql
# Roles
atlas_startup_roles_load_duration_seconds{quantile="0.5"}

# Policies
atlas_startup_policy_load_duration_seconds{quantile="0.5"}

# User Store
atlas_startup_userstore_load_duration_seconds{quantile="0.5"}
```

#### 5. Startup Slowness Alert (Alert Rule)
```promql
# Alert if total startup exceeds 5 minutes
atlas_startup_services_total_duration_seconds{quantile="0.99"} > 300
```

---

## Metric Labels

### Service Label Values

The `service` label in `atlas_startup_service_duration_seconds` can have the following values (depending on configuration):

| Service Name | Description |
|--------------|-------------|
| `ActiveInstanceElectorService` | HA leader election service |
| `AtlasPatchService` | Database patch/migration service |
| `TaskManagement` | Background task management |
| `NotificationHookConsumer` | Kafka notification consumer |
| `ESBasedAuditRepository` | Elasticsearch audit repository |
| `AuthPoliciesBootstrapper` | Authorization policies bootstrapper |
| `IndexRecoveryService` | Index recovery service |

---

## Sample Grafana Dashboard JSON

```json
{
  "title": "Atlas Startup Metrics",
  "panels": [
    {
      "title": "Total Startup Time",
      "type": "stat",
      "targets": [
        {
          "expr": "atlas_startup_services_total_duration_seconds{quantile=\"0.5\"}",
          "legendFormat": "Startup Time"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "s"
        }
      }
    },
    {
      "title": "Service Startup Breakdown",
      "type": "bargauge",
      "targets": [
        {
          "expr": "atlas_startup_service_duration_seconds{quantile=\"0.5\"}",
          "legendFormat": "{{service}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "s"
        }
      }
    },
    {
      "title": "Initialization Phases",
      "type": "timeseries",
      "targets": [
        {
          "expr": "atlas_startup_graph_index_init_duration_seconds{quantile=\"0.5\"}",
          "legendFormat": "Graph Index Init"
        },
        {
          "expr": "atlas_startup_typedef_load_duration_seconds{quantile=\"0.5\"}",
          "legendFormat": "TypeDef Load"
        },
        {
          "expr": "atlas_startup_policy_load_duration_seconds{quantile=\"0.5\"}",
          "legendFormat": "Policy Load"
        },
        {
          "expr": "atlas_startup_roles_load_duration_seconds{quantile=\"0.5\"}",
          "legendFormat": "Roles Load"
        },
        {
          "expr": "atlas_startup_userstore_load_duration_seconds{quantile=\"0.5\"}",
          "legendFormat": "UserStore Load"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "s"
        }
      }
    }
  ]
}
```

---

## Troubleshooting

### Common Startup Bottlenecks

| High Metric | Possible Cause | Resolution |
|-------------|----------------|------------|
| `atlas_startup_graph_index_init_duration_seconds` | Elasticsearch cluster slow/unavailable | Check ES cluster health |
| `atlas_startup_typedef_load_duration_seconds` | Large number of custom types | Optimize model files |
| `atlas_startup_policy_load_duration_seconds` | Large policy set or slow policy admin | Check policy admin connectivity |
| `atlas_startup_roles_load_duration_seconds` | Large number of roles | Consider caching roles |
| `atlas_startup_userstore_load_duration_seconds` | Large user/group store | Consider pagination |

### Useful Commands

```bash
# Check Prometheus endpoint
curl -s http://localhost:21000/api/atlas/admin/metrics/prometheus | grep atlas_startup

# Filter for specific metric
curl -s http://localhost:21000/api/atlas/admin/metrics/prometheus | grep atlas_startup_service_duration
```

