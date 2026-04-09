---
name: Trino Extractor
route: /TrinoExtractor
menu: Documentation
submenu: Tools
---

import  themen  from 'theme/styles/styled-colors';
import  * as theme  from 'react-syntax-highlighter/dist/esm/styles/hljs';
import SyntaxHighlighter from 'react-syntax-highlighter';

# Trino Extractor

## Overview

The Trino Extractor is a comprehensive metadata extraction utility designed for Apache Atlas integration with Trino. It provides discovery, extraction, and synchronization of Trino metadata including catalogs, schemas, tables, and columns into Apache Atlas for enhanced data governance and metadata management.

## Key Features

### Metadata Extraction
* **Comprehensive Discovery**: Automatically discovers and extracts metadata from Trino catalogs, schemas, tables, and columns
* **JDBC-Based Connection**: Uses standard Trino JDBC driver for reliable connectivity
* **Selective Extraction**: Supports extraction for specific catalog, schema, or table names

### Atlas Integration
* **Entity Management**: Creates and updates Atlas entities for Trino metadata objects
* **Relationship Mapping**: Establishes proper hierarchical relationships between catalogs, schemas, tables, and columns
* **Synchronization**: Maintains consistency by removing Atlas entities that no longer exist in Trino
* **Connector Support**: Specialized handling for Trino connectors for which Atlas captures the metadata through individual Hook like Hive, Iceberg

### Scheduling & Automation
* **Cron-based Scheduling**: Supports automated periodic extraction using cron expressions
* **One-time Execution**: Can be run as a single extraction job
* **Error Handling**: Robust error handling with detailed logging

## Architecture

<SyntaxHighlighter language="text" style={theme.github}>
{`
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Trino Cluster │    │ Trino Extractor │    │  Apache Atlas   │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │  Catalogs   │◄┼────┼►│ JDBC Client │ │    │ │  Entities   │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │   Schemas   │ │    │ │ Extraction  │◄┼────┼►│Relationships│ │
│ └─────────────┘ │    │ │   Service   │ │    │ └─────────────┘ │
│ ┌─────────────┐ │    │ └─────────────┘ │    │ ┌─────────────┐ │
│ │   Tables    │ │    │ ┌─────────────┐ │    │ │  Lineage    │ │
│ └─────────────┘ │    │ │Atlas Client │◄┼────┼►│    Data     │ │
│ ┌─────────────┐ │    │ └─────────────┘ │    │ └─────────────┘ │
│ │   Columns   │ │    │                 │    │                 │
│ └─────────────┘ │    └─────────────────┘    └─────────────────┘
└─────────────────┘                           
`}
</SyntaxHighlighter>

## Quick Start

### 1. Configuration Setup

Configure the `atlas-trino-extractor.properties` file:

<SyntaxHighlighter language="properties" style={theme.github}>
{`
# Atlas connection
atlas.rest.address=http://localhost:21000/
# Trino connection
atlas.trino.jdbc.address=jdbc:trino://localhost:8080/
atlas.trino.jdbc.user=your-username
# Catalogs to extract
atlas.trino.catalogs.registered=hive_catalog,iceberg_catalog
`}
</SyntaxHighlighter>

### 2. Basic Execution

<SyntaxHighlighter language="bash" style={theme.github}>
{`
# Extract all registered catalogs
./bin/run-trino-extractor.sh
# Extract specific catalog
./bin/run-trino-extractor.sh -c my_catalog
# Schedule periodic extraction (every 6 hours)
./bin/run-trino-extractor.sh -cx "0 0 */6 * * ?"
`}
</SyntaxHighlighter>

## Configuration Properties

| Property | Description | Default | Example |
|----------|-------------|---------|---------|
| `atlas.rest.address` | Atlas REST API endpoint | `http://localhost:21000/` | `https://atlas.company.com:21443/` |
| `atlas.trino.jdbc.address` | Trino JDBC URL | - | `jdbc:trino://trino-server:8080/` |
| `atlas.trino.jdbc.user` | Trino username | - | `admin` |
| `atlas.trino.jdbc.password` | Trino password | `""` | `password123` |
| `atlas.trino.namespace` | Trino instance namespace | `cm` | `production-cluster` |
| `atlas.trino.catalogs.registered` | Catalogs to extract | - | `hive,iceberg,mysql` |
| `atlas.trino.catalog.hook.enabled.<catalog-name>` | Hook enabled under atlas for this catalog? | `false` | `true` |
| `atlas.trino.catalog.hook.enabled.<catalog-name>.namespace` | Namespace under Atlas for this Hook | `cm` | `cm` |
| `atlas.trino.extractor.schedule` | Cron expression | - | `0 0 2 * * ?` |

## Command Line Usage

### Available Options

| Option | Long Form | Description | Example |
|--------|-----------|-------------|---------|
| `-c` | `--catalog` | Extract specific catalog | `-c hive_catalog` |
| `-s` | `--schema` | Extract specific schema | `-s sales_data` |
| `-t` | `--table` | Extract specific table | `-t customer_orders` |
| `-cx` | `--cronExpression` | Schedule with cron expression | `-cx "0 0 2 * * ?"` |
| `-h` | `--help` | Display help information | `-h` |

## Connector-Specific Processing

#### For Example: Hive Connector Integration
<SyntaxHighlighter language="properties" style={theme.github}>
{`
# Enable Hive hook integration
atlas.trino.catalog.hook.enabled.hive_catalog=true
atlas.trino.catalog.hook.enabled.hive_catalog.namespace=cm
`}
</SyntaxHighlighter>

#### Benefits:
- Links Trino entities with existing Hive entities
- Maintains consistency between Hive and Trino metadata
- Supports environments with Atlas Hive hooks

## FAQ

### Troubleshooting Questions

**Q: Why are some entities not appearing in Atlas?**

A: Check catalog registration, permissions, and network connectivity. Review logs for specific errors.

**Q: How do I handle large clusters with thousands of tables?**

A: Use selective extraction, increase memory allocation, schedule during off-peak hours, and process catalogs individually.

### Documentation
- [Apache Atlas Documentation](https://atlas.apache.org/#/)
- [Trino Documentation](https://trino.io/docs/)
- [Atlas REST API Reference](https://atlas.apache.org/api/v2/)


