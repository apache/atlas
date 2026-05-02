---
name: WhatsNew-2.5
route: /WhatsNew-2.5
menu: Downloads
submenu: Whats New
---

# What's new in Apache Atlas 2.5?

## Features
* **Auto Purge**: Atlas Auto Purge capability with UI support to display auto-purge settings
* **Trino**: addon to extract metadata from Trino periodically
* **Import**: Async Import using Kafka; concurrent import requests with retry locking; asynchronous import during startup
* **Graph / persistence**: support for RDBMS backend and RDBMS-backed audit repository
* **UI**: React UI as the default landing experience; Lineage experience migrated from Backbone.js to React; legacy Backbone.js “New UI” removed
* **Configuration**: support to load custom application properties; option to customize the configuration filename; optional disabling of composite indexes

## Enhancements
* **Platform**
  * Upgrade to Hadoop 3.4.2 and HBase 2.6.x
  * Upgrade JanusGraph to 1.1.0
  * Replace HBase dependencies with hbase-shaded-*-hadoop3 variants
* **Dependencies**
  * Upgrades across server and UI modules (Spring LDAP, Netty, commons-io, Swagger UI, DomPurify, POI)
* **Search**
  * Pagination when fetching relationships of an entity
  * Fixes for Relationship Search and Basic Search approximateCount
  * searchRelatedEntities when relationship labels or multi-type mappings apply
  * Timerange search when client and server timezones differ
* **Hive**
  * Entities for tables on OFS/O3FS buckets/volumes
  * Fix for import-hive.sh when shell entity IDs appear during import
* **Spark**
  * Options to ignore or shorten spark_process attributes
* **Impala**
  * Lineage for SQL using WITH clauses
* **Kafka**
  * Notification metrics include average processing time per topic-partition
* **Export / Import**
  * Incremental export fixes for propagated classifications
  * Optional transforms for HDFS paths during import
  * Index recovery and graph transaction handling improvements
* **High availability**
  * Curator used only when Atlas runs in HA mode
* **Hooks**
  * Fixes for ignore/prune patterns on rename messages and partial Hive-table ignores
* **TLS**
  * Support for TLS 1.3
* [List of JIRAs resolved in Apache Atlas 2.5.0 release](https://issues.apache.org/jira/issues/?jql=project%20%3D%20ATLAS%20AND%20fixVersion%20%3D%202.5.0%20ORDER%20BY%20key%20DESC)
