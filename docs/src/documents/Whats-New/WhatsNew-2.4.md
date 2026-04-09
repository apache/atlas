---
name: WhatsNew-2.4
route: /WhatsNew-2.4
menu: Downloads
submenu: Whats New
---

# What's new in Apache Atlas 2.4?

## Features
* **Search**: added support to download the search results of Basic and Advanced search.
* **Hook**: added CouchBase bridge
* **Audits**: implemented aging for audits
* **Notification**: utility to analyze hook notifications
* **Ignore/Prune Pattern** : support ignore patterns to be generic for all the hooks
* Provide Liveness and Readyness probes

## Enhancements
* **Export/Import**: fixes and enhancements in this feature
* **Dynamic Index Recovery**: improvements in handling index recovery dynamically
* **Relationship**: performance improvements in dealing with large number of relationhips
* **Lineage**: performance improvements by handling data where there is no scope of lineage.
* **Notification Hook Consumer**: message processing improvement by skipping to retry for specific cases
* **Hbase Compression Algotithm**: identified SNAPPY compression performs faster, hence support is added to change compression
* **Search**: support for Chinese character in Atlas entities is added
* **Dependencies Upgrade**: JanusGraph, Spring Security, Netty, Tinkerpop, Spring Framework, Hbase, Sqoop, Storm, Jetty, Testng
* **UI Dependencies Upgrade**: Swagger-UI, DomPurify, send, serve-static, elliptic
* **UI**: fixes and improvements in multiple areas like Glossary, Entity Detail Page, Property tab, Text Editor, Search
* **Log**: replace use of log4j with logback
* **Docker image**: improvements to Docker support
* [List of JIRAs resolved in Apache Atlas 2.4.0 release](https://issues.apache.org/jira/issues/?jql=project%20%3D%20ATLAS%20AND%20fixVersion%20%3D%202.4.0%20ORDER%20BY%20key%20DESC)
