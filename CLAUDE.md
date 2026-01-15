# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Atlas Metastore is a fork of Apache Atlas - a metadata governance framework for data cataloging, lineage tracking, and access control. It uses JanusGraph for graph-based metadata storage with Cassandra backend and Elasticsearch for search.

## Prerequisites

Run the prerequisites check script:
```bash
./scripts/check-prerequisites.sh
```

### Required Setup

1. **Java 17** - The `.mavenrc` file auto-sets JAVA_HOME for Maven builds
2. **GitHub PAT** - Required for JanusGraph dependencies (private package)
   ```bash
   export GITHUB_USERNAME=<your-github-username>
   export GITHUB_TOKEN=<your-pat-with-read:packages-scope>
   ```
   **Note:** Your GitHub account must have access to the `atlanhq` organization

3. **Keycloak dependency** (one-time):
   ```bash
   mkdir -p ~/.m2/repository/org/keycloak
   curl https://atlan-public.s3.eu-west-1.amazonaws.com/artifact/keycloak-15.0.2.1.zip -o keycloak-15.0.2.1.zip
   unzip -o keycloak-15.0.2.1.zip -d ~/.m2/repository/org
   ```

4. **Maven settings.xml** (`~/.m2/settings.xml`):
   ```xml
   <servers>
     <server>
       <id>github</id>
       <username>${env.GITHUB_USERNAME}</username>
       <password>${env.GITHUB_TOKEN}</password>
     </server>
   </servers>
   ```

## Build Commands

```bash
# Intel Mac
mvn clean -Dmaven.test.skip -DskipTests -Drat.skip=true -DskipOverlay -DskipEnunciate=true install -Pdist

# Apple Silicon Mac
mvn clean -Dos.detected.classifier=osx-x86_64 -Dmaven.test.skip -DskipTests -Drat.skip=true -DskipOverlay -DskipEnunciate=true install -Pdist

# Full build with tests
export MAVEN_OPTS="-Xms2g -Xmx2g"
mvn clean install
mvn clean package -Pdist
```

**Important:** Use `mvn install` (not just `package`) so IntelliJ can resolve local module dependencies.

## Running Tests

```bash
# Run all tests in a module
mvn test -pl webapp

# Run a specific test class
mvn test -pl webapp -Dtest=AtlasDockerIntegrationTest

# Integration tests (requires Docker)
./run-integration-tests.sh                    # Full run
./run-integration-tests.sh --skip-build       # Skip Maven build
./run-integration-tests.sh --keep-containers  # Keep containers after tests
./run-integration-tests.sh --debug            # Enable debug logging
```

Test logs are saved to `target/test-logs/`.

## Local Development Setup

### 1. Start Docker Runtime (macOS)
```bash
colima start --disk 10 --memory 4
```

### 2. Start Infrastructure Services
```bash
docker-compose -f local-dev/docker-compose.yaml up -d
```
Services: Redis (6379), Cassandra (9042), Elasticsearch (9200)

### 3. Redis Sentinel Config (already set in deploy/conf)
The local config has `atlas.redis.sentinel.check_list.enabled=false` which disables sentinel list checking for local development.

### 4. Run Atlas (IntelliJ)

| Setting | Value |
|---------|-------|
| Main Class | `org.apache.atlas.Atlas` |
| Module | `java-17` |
| Classpath | `atlas-webapp` |

**VM Options:**
```
--add-opens java.base/java.lang=ALL-UNNAMED
-Datlas.home=deploy/
-Datlas.conf=deploy/conf
-Datlas.data=deploy/data
-Datlas.log.dir=deploy/logs
-Dembedded.solr.directory=deploy/data
-Dzookeeper.snapshot.trust.empty=true
-Dlogback.configurationFile=file:./deploy/conf/atlas-logback.xml
```

Atlas runs at http://localhost:21000

## Architecture

### Module Structure

| Module | Purpose |
|--------|---------|
| `webapp` | REST API controllers (`/web/rest/`) |
| `repository` | Core services - entity discovery, audit, search |
| `graphdb/janus` | JanusGraph + Elasticsearch integration |
| `common` | Shared utilities, Redis service, feature flags |
| `authorization` | RBAC/ABAC via Apache Ranger |
| `notification` | Kafka-based async notifications |

### Key Code Paths

- **REST Controllers:** `webapp/src/main/java/org/apache/atlas/web/rest/`
- **Discovery Service:** `repository/src/main/java/org/apache/atlas/discovery/EntityDiscoveryService.java`
- **Graph DB Layer:** `graphdb/janus/src/main/java/org/apache/atlas/repository/graphdb/janus/`
- **Feature Flags:** `common/src/main/java/org/apache/atlas/service/FeatureFlagStore.java`

### Data Flow

Request → REST Controller → Service Layer → JanusGraph/Cassandra → Elasticsearch (for search) → Redis (for caching/flags)

### Search System

- `POST /search/indexsearch` - Atlas-shaped DSL search
- `POST /search/es` - Raw Elasticsearch passthrough
- `POST /search/count` - Count queries

Search routing depends on `RequestContext.clientOrigin` - UI requests go to UI cluster, others to non-UI cluster.

### Feature Flags

Redis-backed with dual caching (primary TTL cache + permanent fallback). Application fails fast if Redis unavailable at startup.

```java
// Check flag value
boolean isEnabled = FeatureFlagStore.evaluate("ENABLE_JANUS_OPTIMISATION", "true");
```

REST API: `GET/PUT/DELETE /api/atlas/v2/featureflags/{key}`

Current flags: `ENABLE_JANUS_OPTIMISATION`, `enable_persona_hierarchy_filter`, `DISABLE_WRITE_FLAG`, `discovery_use_dsl_optimisation`

## Key Patterns

### Feature Flag Implementation
- All flags defined in `FeatureFlag` enum with explicit keys and defaults
- Dual cache: time-bound primary (30min TTL) + permanent fallback
- Use `synchronized` methods for write operations
- Fail-fast startup validation

### Search Optimization
- DSL optimization controlled by `discovery_use_dsl_optimisation` flag
- Only applied when `RequestContext.clientOrigin == product_webapp`
- Request isolation selects ES client based on client origin

### Spring Integration
- Use `@DependsOn("redisService")` for proper initialization order
- Use `@PostConstruct` for initialization with validation
- `ApplicationContextProvider` pattern for static method access to Spring beans

## Technology Stack

- Java 17 (OpenJDK)
- Maven 3.8+
- Spring 5.3.18 / Spring Security 5.5.1
- JanusGraph 0.5.3 with Cassandra 2.1
- Elasticsearch 7.16.2
- Redis 6.2.14
- Jersey 1.19 (REST)

## Troubleshooting

### Default Credentials
- Username: `admin`
- Password: `admin`
- Credentials file: `deploy/conf/users-credentials.properties` (SHA-256 hashed)

### Corrupted Maven Cache
If you see `Invalid CEN header` or ZIP errors:
```bash
rm -rf ~/.m2/repository/org/aspectj/aspectjweaver/1.8.9
mvn dependency:resolve -U
```

### Cassandra Tags Keyspace Error

If you see `"errorMessage": "Error fetching all classifications"`, recreate the tags keyspace:

```bash
docker exec -it $(docker-compose -f local-dev/docker-compose.yaml ps -q cassandra) cqlsh
```

```sql
DROP KEYSPACE IF EXISTS tags;
CREATE KEYSPACE tags WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
USE tags;

CREATE TABLE tags_by_id (
    bucket int, id text, is_propagated boolean, source_id text, tag_type_name text,
    asset_metadata text, is_deleted boolean, tag_meta_json text, updated_at timestamp,
    PRIMARY KEY ((bucket, id), is_propagated, source_id, tag_type_name)
);

CREATE TABLE propagated_tags_by_source (
    source_id text, tag_type_name text, propagated_asset_id text,
    asset_metadata text, updated_at timestamp,
    PRIMARY KEY ((source_id, tag_type_name), propagated_asset_id)
);
```

### Full Environment Reset

```bash
docker-compose -f local-dev/docker-compose.yaml down
docker system prune -a --volumes -f
colima stop
```
