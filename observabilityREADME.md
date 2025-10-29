# Atlas Observability Metrics Documentation

This document provides comprehensive documentation for all metrics exposed by the `AtlasObservabilityService` class, including their types, purposes, and corresponding Grafana queries.

## Metric Prefix
All metrics use the prefix: `atlas_observability_bulk_`

## Metrics Overview

| Metric Name | Type | Purpose | Tags | Unit |
|-------------|------|---------|------|------|
| `operations_in_progress` | Gauge | Current number of Atlas operations in progress | `component=observability` | count |
| `total_operations` | Gauge | Total number of Atlas operations processed | `component=observability` | count |
| `duration` | Timer | Duration of create/update operations | `client_origin` | milliseconds |
| `payload_size` | DistributionSummary | Size of payload assets | `client_origin` | items |
| `payload_bytes` | DistributionSummary | Size of payload in bytes | `client_origin` | bytes |
| `array_relationships` | DistributionSummary | Total count of array relationships | `client_origin` | items |
| `relationship_attributes` | Counter | Count of relationship attributes by type | `client_origin`, `relationship_name` | count |
| `append_relationship_attributes` | Counter | Count of append relationship attributes by type | `client_origin`, `relationship_name` | count |
| `remove_relationship_attributes` | Counter | Count of remove relationship attributes by type | `client_origin`, `relationship_name` | count |
| `array_attributes` | DistributionSummary | Total count of array attributes | `client_origin` | items |
| `attributes` | Counter | Count of array attributes by name | `client_origin`, `attribute_name` | count |
| `diff_calc_time` | Timer | Time spent calculating differences | `client_origin` | milliseconds |
| `lineage_calc_time` | Timer | Time spent calculating lineage | `client_origin` | milliseconds |
| `validation_time` | Timer | Time spent on validation | `client_origin` | milliseconds |
| `ingestion_time` | Timer | Time spent on ingestion | `client_origin` | milliseconds |
| `notification_time` | Timer | Time spent on notifications | `client_origin` | milliseconds |
| `audit_log_time` | Timer | Time spent on audit logging | `client_origin` | milliseconds |
| `operations` | Counter | Count of operations by type and status | `client_origin`, `operation`, `status` | count |
| `operation_errors` | Counter | Count of operation errors by type | `client_origin`, `operation`, `error_type` | count |

## Detailed Metric Descriptions

### 1. Operations Monitoring

#### `atlas_observability_bulk_operations_in_progress`
- **Type**: Gauge
- **Purpose**: Monitors current load by tracking active operations
- **Tags**: `component=observability`
- **Use Case**: Load balancing, capacity planning, performance monitoring

#### `atlas_observability_bulk_total_operations`
- **Type**: Gauge
- **Purpose**: Tracks total operations processed since service start
- **Tags**: `component=observability`
- **Use Case**: Throughput monitoring, service health

### 2. Performance Metrics

#### `atlas_observability_bulk_duration`
- **Type**: Timer
- **Purpose**: Measures end-to-end duration of create/update operations
- **Tags**: `client_origin`
- **Use Case**: SLA monitoring, performance optimization
- **SLOs**: 100ms, 500ms, 1s, 5s, 10s, 30s, 1m, 5m, 15m, 30m, 1h, 2h

#### `atlas_observability_bulk_payload_size`
- **Type**: DistributionSummary
- **Purpose**: Tracks number of assets in payload
- **Tags**: `client_origin`
- **Use Case**: Batch size optimization, capacity planning
- **SLOs**: 1, 10, 100, 1K, 10K, 100K, 1M, 10M items

#### `atlas_observability_bulk_payload_bytes`
- **Type**: DistributionSummary
- **Purpose**: Tracks payload size in bytes
- **Tags**: `client_origin`
- **Use Case**: Network optimization, memory usage monitoring
- **SLOs**: 1KB, 10KB, 100KB, 1MB, 10MB, 100MB, 1GB, 10GB

### 3. Relationship Metrics

#### `atlas_observability_bulk_array_relationships`
- **Type**: DistributionSummary
- **Purpose**: Tracks total array relationships processed
- **Tags**: `client_origin`
- **Use Case**: Data complexity analysis, performance optimization

#### `atlas_observability_bulk_relationship_attributes`
- **Type**: Counter
- **Purpose**: Counts relationship attributes by type
- **Tags**: `client_origin`, `relationship_name`
- **Use Case**: Relationship usage analysis, data modeling insights

#### `atlas_observability_bulk_append_relationship_attributes`
- **Type**: Counter
- **Purpose**: Counts append relationship attributes by type
- **Tags**: `client_origin`, `relationship_name`
- **Use Case**: Data modification pattern analysis

#### `atlas_observability_bulk_remove_relationship_attributes`
- **Type**: Counter
- **Purpose**: Counts remove relationship attributes by type
- **Tags**: `client_origin`, `relationship_name`
- **Use Case**: Data cleanup pattern analysis

### 4. Attribute Metrics

#### `atlas_observability_bulk_array_attributes`
- **Type**: DistributionSummary
- **Purpose**: Tracks total array attributes processed
- **Tags**: `client_origin`
- **Use Case**: Data complexity analysis

#### `atlas_observability_bulk_attributes`
- **Type**: Counter
- **Purpose**: Counts array attributes by name
- **Tags**: `client_origin`, `attribute_name`
- **Use Case**: Attribute usage analysis, schema evolution

### 5. Timing Metrics

#### `atlas_observability_bulk_diff_calc_time`
- **Type**: Timer
- **Purpose**: Time spent calculating differences between entities
- **Tags**: `client_origin`
- **Use Case**: Performance optimization, bottleneck identification

#### `atlas_observability_bulk_lineage_calc_time`
- **Type**: Timer
- **Purpose**: Time spent calculating lineage relationships
- **Tags**: `client_origin`
- **Use Case**: Lineage performance monitoring

#### `atlas_observability_bulk_validation_time`
- **Type**: Timer
- **Purpose**: Time spent on entity validation
- **Tags**: `client_origin`
- **Use Case**: Validation performance monitoring

#### `atlas_observability_bulk_ingestion_time`
- **Type**: Timer
- **Purpose**: Time spent on data ingestion
- **Tags**: `client_origin`
- **Use Case**: Ingestion performance monitoring

#### `atlas_observability_bulk_notification_time`
- **Type**: Timer
- **Purpose**: Time spent on notification processing
- **Tags**: `client_origin`
- **Use Case**: Notification performance monitoring

#### `atlas_observability_bulk_audit_log_time`
- **Type**: Timer
- **Purpose**: Time spent on audit logging
- **Tags**: `client_origin`
- **Use Case**: Audit performance monitoring

### 6. Operation Metrics

#### `atlas_observability_bulk_operations`
- **Type**: Counter
- **Purpose**: Counts operations by type and status
- **Tags**: `client_origin`, `operation`, `status`
- **Use Case**: Success/failure rate monitoring, operation analysis

#### `atlas_observability_bulk_operation_errors`
- **Type**: Counter
- **Purpose**: Counts operation errors by type
- **Tags**: `client_origin`, `operation`, `error_type`
- **Use Case**: Error analysis, debugging, reliability monitoring

## Grafana Queries

### 1. Operations Monitoring

#### Current Operations in Progress
```promql
atlas_observability_bulk_operations_in_progress
```

#### Total Operations Processed
```promql
atlas_observability_bulk_total_operations
```

#### Operations Rate (per minute)
```promql
rate(atlas_observability_bulk_total_operations[1m])
```

### 2. Performance Metrics

#### Average Duration by Client Origin
```promql
rate(atlas_observability_bulk_duration_seconds_sum[5m]) / rate(atlas_observability_bulk_duration_seconds_count[5m])
```

#### 95th Percentile Duration
```promql
histogram_quantile(0.95, rate(atlas_observability_bulk_duration_seconds_bucket[5m]))
```

#### Duration SLO Compliance (operations under 1 second)
```promql
rate(atlas_observability_bulk_duration_seconds_bucket{le="1.0"}[5m]) / rate(atlas_observability_bulk_duration_seconds_count[5m])
```

#### Average Payload Size
```promql
rate(atlas_observability_bulk_payload_size_sum[5m]) / rate(atlas_observability_bulk_payload_size_count[5m])
```

#### 95th Percentile Payload Size
```promql
histogram_quantile(0.95, rate(atlas_observability_bulk_payload_size_bucket[5m]))
```

#### Average Payload Bytes
```promql
rate(atlas_observability_bulk_payload_bytes_sum[5m]) / rate(atlas_observability_bulk_payload_bytes_count[5m])
```

#### 95th Percentile Payload Bytes
```promql
histogram_quantile(0.95, rate(atlas_observability_bulk_payload_bytes_bucket[5m]))
```

### 3. Relationship Metrics

#### Total Array Relationships Rate
```promql
rate(atlas_observability_bulk_array_relationships_count[5m])
```

#### Average Array Relationships per Operation
```promql
rate(atlas_observability_bulk_array_relationships_sum[5m]) / rate(atlas_observability_bulk_array_relationships_count[5m])
```

#### Relationship Attributes by Type (Top 10)
```promql
topk(10, rate(atlas_observability_bulk_relationship_attributes_total[5m]))
```

#### Append Relationship Attributes Rate
```promql
rate(atlas_observability_bulk_append_relationship_attributes_total[5m])
```

#### Remove Relationship Attributes Rate
```promql
rate(atlas_observability_bulk_remove_relationship_attributes_total[5m])
```

### 4. Attribute Metrics

#### Total Array Attributes Rate
```promql
rate(atlas_observability_bulk_array_attributes_count[5m])
```

#### Average Array Attributes per Operation
```promql
rate(atlas_observability_bulk_array_attributes_sum[5m]) / rate(atlas_observability_bulk_array_attributes_count[5m])
```

#### Attributes by Name (Top 10)
```promql
topk(10, rate(atlas_observability_bulk_attributes_total[5m]))
```

### 5. Timing Metrics

#### Average Diff Calculation Time
```promql
rate(atlas_observability_bulk_diff_calc_time_seconds_sum[5m]) / rate(atlas_observability_bulk_diff_calc_time_seconds_count[5m])
```

#### 95th Percentile Diff Calculation Time
```promql
histogram_quantile(0.95, rate(atlas_observability_bulk_diff_calc_time_seconds_bucket[5m]))
```

#### Average Lineage Calculation Time
```promql
rate(atlas_observability_bulk_lineage_calc_time_seconds_sum[5m]) / rate(atlas_observability_bulk_lineage_calc_time_seconds_count[5m])
```

#### Average Validation Time
```promql
rate(atlas_observability_bulk_validation_time_seconds_sum[5m]) / rate(atlas_observability_bulk_validation_time_seconds_count[5m])
```

#### Average Ingestion Time
```promql
rate(atlas_observability_bulk_ingestion_time_seconds_sum[5m]) / rate(atlas_observability_bulk_ingestion_time_seconds_count[5m])
```

#### Average Notification Time
```promql
rate(atlas_observability_bulk_notification_time_seconds_sum[5m]) / rate(atlas_observability_bulk_notification_time_seconds_count[5m])
```

#### Average Audit Log Time
```promql
rate(atlas_observability_bulk_audit_log_time_seconds_sum[5m]) / rate(atlas_observability_bulk_audit_log_time_seconds_count[5m])
```

### 6. Operation Metrics

#### Operations by Status
```promql
rate(atlas_observability_bulk_operations_total[5m])
```

#### Success Rate by Operation Type
```promql
rate(atlas_observability_bulk_operations_total{status="success"}[5m]) / rate(atlas_observability_bulk_operations_total[5m])
```

#### Failure Rate by Operation Type
```promql
rate(atlas_observability_bulk_operations_total{status="failure"}[5m]) / rate(atlas_observability_bulk_operations_total[5m])
```

#### Error Rate by Error Type
```promql
rate(atlas_observability_bulk_operation_errors_total[5m])
```

#### Top Error Types
```promql
topk(10, rate(atlas_observability_bulk_operation_errors_total[5m]))
```

### 7. Client Origin Analysis

#### Operations by Client Origin
```promql
rate(atlas_observability_bulk_operations_total[5m]) by (client_origin)
```

#### Average Duration by Client Origin
```promql
rate(atlas_observability_bulk_duration_seconds_sum[5m]) by (client_origin) / rate(atlas_observability_bulk_duration_seconds_count[5m]) by (client_origin)
```

#### Payload Size by Client Origin
```promql
rate(atlas_observability_bulk_payload_size_sum[5m]) by (client_origin) / rate(atlas_observability_bulk_payload_size_count[5m]) by (client_origin)
```

### 8. SLO Monitoring

#### Duration SLO Compliance (95% under 5 seconds)
```promql
histogram_quantile(0.95, rate(atlas_observability_bulk_duration_seconds_bucket[5m])) < 5
```

#### Payload Size SLO Compliance (95% under 1MB)
```promql
histogram_quantile(0.95, rate(atlas_observability_bulk_payload_bytes_bucket[5m])) < 1048576
```

#### Success Rate SLO (99% success rate)
```promql
rate(atlas_observability_bulk_operations_total{status="success"}[5m]) / rate(atlas_observability_bulk_operations_total[5m]) > 0.99
```

## Dashboard Recommendations

### 1. Overview Dashboard
- Operations in progress (gauge)
- Total operations (gauge)
- Operations rate (line chart)
- Success rate (stat panel)
- Average duration (stat panel)

### 2. Performance Dashboard
- Duration percentiles (line chart)
- Payload size distribution (histogram)
- Timing breakdown (stacked bar chart)
- SLO compliance (stat panels)

### 3. Client Analysis Dashboard
- Operations by client origin (pie chart)
- Performance by client origin (table)
- Error rate by client origin (bar chart)

### 4. Error Analysis Dashboard
- Error rate over time (line chart)
- Top error types (bar chart)
- Error rate by operation type (table)
- Error rate by client origin (table)

## Alerting Rules

### Critical Alerts
```yaml
- alert: HighErrorRate
  expr: rate(atlas_observability_bulk_operations_total{status="failure"}[5m]) / rate(atlas_observability_bulk_operations_total[5m]) > 0.05
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "High error rate detected"

- alert: HighLatency
  expr: histogram_quantile(0.95, rate(atlas_observability_bulk_duration_seconds_bucket[5m])) > 10
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "High latency detected"

- alert: OperationsInProgressHigh
  expr: atlas_observability_bulk_operations_in_progress > 100
  for: 1m
  labels:
    severity: warning
  annotations:
    summary: "High number of operations in progress"
```

### Warning Alerts
```yaml
- alert: LargePayloadSize
  expr: histogram_quantile(0.95, rate(atlas_observability_bulk_payload_bytes_bucket[5m])) > 10485760
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Large payload sizes detected"

- alert: SlowValidation
  expr: rate(atlas_observability_bulk_validation_time_seconds_sum[5m]) / rate(atlas_observability_bulk_validation_time_seconds_count[5m]) > 2
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Slow validation detected"
```