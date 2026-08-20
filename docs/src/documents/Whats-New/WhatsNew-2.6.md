---
name: WhatsNew-2.6
route: /WhatsNew-2.6
menu: Downloads
submenu: Whats New
---

# What's new in Apache Atlas 2.6?

## Features
* **React UI**: Dashboard Overview; Vite-based build optimizations and lazy route loading; broad React UI coverage for search, glossary, lineage, audit, and administration
* **Authentication**: JWT authentication support; header-based authentication for trusted proxy / gateway deployments
* **Notifications**: Distributed notification processing
* **Export / Import**: Concurrent ingest; export/import improvements for tag attributes and relationship types
* **Propagation**: Rename and delete propagation support
* **Graph / persistence**: RDBMS backend and audit repository support (with follow-on fixes)
* **Admin / Purge**: Authorization on Admin REST endpoints; Auto Purge UI improvements
* **Docker**: Immutable container setup updates

## Enhancements
* **Platform**
  * Upgrade to Hadoop 3.4.2 and HBase 2.6.x
  * Upgrade JanusGraph to 1.1.0
  * Migrate from commons-configuration1 to commons-configuration2
  * Replace HBase dependencies with hbase-shaded-*-hadoop3 variants
* **Dependencies**
  * Netty, dompurify, handlebars, and frontend npm dependency upgrades
* **Search**
  * CONTAINS query handling with JanusGraph indexing improvements
* **Hooks**
  * Impala lineage fix for self-referencing INSERT OVERWRITE
  * Ignore/rename pattern fixes
* **Export / Import**
  * Incremental export fixes; import script and Kafka import improvements
* **UI**
  * Classic/React UI coexistence fixes; relationship tab UX; glossary and business metadata fixes
* **Security**
  * XSS fix in sanitize-html; Swagger apidocs static asset access adjustment
* **Release / build**
  * Release artifact checksums; WAR size and build stabilization for atlas-2.6
* [List of JIRAs resolved in Apache Atlas 2.6.0 release](https://issues.apache.org/jira/issues/?jql=project%20%3D%20ATLAS%20AND%20fixVersion%20%3D%202.6.0%20ORDER%20BY%20key%20DESC)
