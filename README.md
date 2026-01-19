# Atlas Metastore

Atlan's fork of [Apache Atlas](https://atlas.apache.org/) - a metadata governance framework for data cataloging, lineage tracking, and access control.

## Overview

Atlas Metastore provides:
- **Metadata Management** - Centralized metadata store for data assets
- **Data Lineage** - Track data flow and transformations
- **Data Discovery** - Search and explore data assets
- **Access Control** - Role-based (RBAC) and attribute-based (ABAC) security via Apache Ranger

## Tech Stack

- **Language:** Java 17
- **Build:** Maven
- **Graph Database:** JanusGraph with Cassandra backend
- **Search:** Elasticsearch
- **Cache:** Redis with Sentinel
- **REST Framework:** Jersey

## Quick Start

### Prerequisites

Run the prerequisites check:
```bash
./scripts/check-prerequisites.sh
```

### Build

```bash
# Apple Silicon Mac
mvn clean -Dos.detected.classifier=osx-x86_64 -Dmaven.test.skip -DskipTests -Drat.skip=true -DskipOverlay -DskipEnunciate=true install -Pdist

# Intel Mac
mvn clean -Dmaven.test.skip -DskipTests -Drat.skip=true -DskipOverlay -DskipEnunciate=true install -Pdist
```

### Run Locally

1. Start infrastructure services:
   ```bash
   docker-compose -f local-dev/docker-compose.yaml up -d
   ```

2. Start Atlas (see [LOCAL_SETUP.md](LOCAL_SETUP.md) for IntelliJ configuration)

3. Verify Atlas is running (UI may not load reliably, use API):
   ```bash
   curl -u admin:admin http://localhost:21000/api/atlas/v2/types/typedefs/headers
   ```

## Documentation

- **[LOCAL_SETUP.md](LOCAL_SETUP.md)** - Complete local development setup guide
- **[CLAUDE.md](CLAUDE.md)** - AI assistant context for this repository
- **[Apache Atlas Docs](https://atlas.apache.org/documentation.html)** - Original Apache Atlas documentation

## Build Artifacts

After a successful build:
```
distro/target/apache-atlas-<version>-server.tar.gz
distro/target/apache-atlas-<version>-bin.tar.gz
```

## License

Licensed under the [Apache License 2.0](LICENSE).
