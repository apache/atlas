# Adding New ConfigMap Properties to Atlas Helm Charts

This guide explains how to add new configuration properties to the Atlas Helm charts for both `atlas` and `atlas-read` deployments.

## Overview

Atlas configuration properties are managed through Helm charts using:
1. **values.yaml** - Defines default values and structure
2. **templates/configmap.yaml** - Generates the `atlas-application.properties` file

Both `atlas` (write) and `atlas-read` (read replica) charts need to be updated when adding shared configuration.

## Files to Modify

| Chart | File | Purpose |
|-------|------|---------|
| atlas | `helm/atlas/values.yaml` | Default values for atlas |
| atlas | `helm/atlas/templates/configmap.yaml` | ConfigMap template for atlas |
| atlas-read | `helm/atlas-read/values.yaml` | Default values for atlas-read |
| atlas-read | `helm/atlas-read/templates/configmap.yaml` | ConfigMap template for atlas-read |

## Step-by-Step Guide

### Step 1: Add Value to `values.yaml`

Add your new configuration under the `atlas:` section in both `helm/atlas/values.yaml` and `helm/atlas-read/values.yaml`.

**Naming Convention:** Use camelCase for YAML keys, organized in logical groups.

```yaml
atlas:
  # ... existing config ...

  # Your new feature configuration
  yourFeature:
    enabled: true           # Boolean flags
    someValue: "default"    # String values
    numericValue: 100       # Numeric values
```

**Example - Simple boolean flag:**
```yaml
atlas:
  deterministicIdGeneration:
    enabled: true
```

**Example - Complex configuration:**
```yaml
atlas:
  configStore:
    cassandra:
      enabled: true
      activated: false
      syncIntervalMs: 60000
      keyspace: config_store
```

### Step 2: Add Property to `configmap.yaml`

Add the property to `templates/configmap.yaml` in both charts. The property is written to `atlas-application.properties`.

**Naming Convention:** Use dot-notation for property names (e.g., `atlas.feature.sub.property`).

**Template Syntax:**
```
atlas.your.property.name={{ .Values.atlas.yourFeature.enabled }}
```

**Add with Section Header:**
```
    ######### Your Feature Name #########
    atlas.your.feature.enabled={{ .Values.atlas.yourFeature.enabled }}
    atlas.your.feature.value={{ .Values.atlas.yourFeature.someValue }}
```

### Step 3: Placement Guidelines

**In `values.yaml`:**
- Place related configurations together
- Add comments explaining the configuration purpose
- Use consistent indentation (2 spaces)

**In `configmap.yaml`:**
- Add a section header comment (`#########`)
- Place at the end of the file (before any closing `{{- end }}` in atlas-read)
- Keep related properties grouped together

## Common Patterns

### Boolean Feature Flags

**values.yaml:**
```yaml
atlas:
  myFeature:
    enabled: false
```

**configmap.yaml:**
```
atlas.my.feature.enabled={{ .Values.atlas.myFeature.enabled }}
```

### Conditional Configuration

**values.yaml:**
```yaml
atlas:
  cache:
    enabled: false
```

**configmap.yaml:**
```
{{- if eq .Values.atlas.cache.enabled true }}
atlas.cache.type=redis
atlas.cache.ttl=3600
{{- end }}
```

### Configuration with Multiple Values

**values.yaml:**
```yaml
atlas:
  indexsearch:
    enable_api_limit: false
    query_size_max_limit: 100000
    enable_async: true
```

**configmap.yaml:**
```
atlas.indexsearch.enable.api.limit={{ .Values.atlas.indexsearch.enable_api_limit }}
atlas.indexsearch.query.size.max.limit={{ .Values.atlas.indexsearch.query_size_max_limit }}
atlas.indexsearch.async.enable={{ .Values.atlas.indexsearch.enable_async }}
```

## Naming Conventions

| Context | Convention | Example |
|---------|------------|---------|
| values.yaml keys | camelCase or snake_case | `deterministicIdGeneration`, `enable_api_limit` |
| Property names | dot.notation.lowercase | `atlas.deterministic.id.generation.enabled` |
| Section headers | Title Case with `#########` | `######### Feature Name #########` |

## Differences Between atlas and atlas-read

| Aspect | atlas | atlas-read |
|--------|-------|------------|
| ConfigMap name | `atlas-config` | `atlas-read-config` |
| Tasks enabled | `true` | `false` |
| Conditional creation | Always created | Only when `svcIsolation`, `esIsolation`, or `globalSvcIsolation` enabled |
| File ending | No wrapper | Wrapped in `{{- if ... }}...{{- end }}` |

**Important:** When adding to `atlas-read/templates/configmap.yaml`, ensure the property is added **before** the closing `{{- end }}` tag.

## Verification

After making changes, verify the Helm template renders correctly:

```bash
# Render atlas template
helm template atlas ./helm/atlas --debug | grep -A5 "your.property"

# Render atlas-read template (with required flags)
helm template atlas-read ./helm/atlas-read \
  --set global.svcIsolation.enabled=true \
  --debug | grep -A5 "your.property"
```

## Checklist

- [ ] Added value to `helm/atlas/values.yaml`
- [ ] Added value to `helm/atlas-read/values.yaml`
- [ ] Added property to `helm/atlas/templates/configmap.yaml`
- [ ] Added property to `helm/atlas-read/templates/configmap.yaml`
- [ ] Values have appropriate defaults
- [ ] Property names follow dot-notation convention
- [ ] Section header comment added
- [ ] Tested template rendering

## Example: Complete Addition

Here's a complete example of adding `atlas.deterministic.id.generation.enabled`:

### 1. values.yaml (both charts)
```yaml
atlas:
  # Deterministic ID generation for entities
  deterministicIdGeneration:
    enabled: true
```

### 2. configmap.yaml (both charts)
```
    ######### Deterministic ID Generation #########
    atlas.deterministic.id.generation.enabled={{ .Values.atlas.deterministicIdGeneration.enabled }}
```

## Related Files

- `helm/atlas/values.yaml` - Atlas default values
- `helm/atlas/templates/configmap.yaml` - Atlas ConfigMap template
- `helm/atlas-read/values.yaml` - Atlas-read default values
- `helm/atlas-read/templates/configmap.yaml` - Atlas-read ConfigMap template
- `helm/atlas/templates/configmap-leangraph.yaml` - Leangraph-specific ConfigMap (if applicable)
