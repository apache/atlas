# Local Development Setup Guide for Atlas

This guide will help you set up Atlas for local development.

## Quick Start

Run the prerequisites check script to validate your setup:
```bash
./scripts/check-prerequisites.sh
```

## Prerequisites

### Required Software
- Java 17 (Recommended: Zulu OpenJDK 17 or Temurin)
- Maven 3.8+
- Docker (via Colima for macOS)
- Git

### 1. Java Setup

Install Java 17:
```bash
brew install openjdk@17
```

The project includes a `.mavenrc` file that automatically sets JAVA_HOME for Maven builds. For other tools, set manually:
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH=$JAVA_HOME/bin:$PATH
```

**Tip:** Use `jenv` for managing multiple Java versions:
```bash
brew install jenv
jenv enable-plugin export
jenv enable-plugin maven
```

### 2. GitHub Package Registry Access

The project uses a custom JanusGraph fork hosted on GitHub Packages. You need:

1. **GitHub account with `atlanhq` organization access**
2. **Personal Access Token** with `read:packages` scope

Set environment variables:
```bash
export GITHUB_USERNAME=<your-github-username>
export GITHUB_TOKEN=<your-pat-token>
```

Create or update `~/.m2/settings.xml`:
```xml
<settings>
  <servers>
    <server>
      <id>github</id>
      <username>${env.GITHUB_USERNAME}</username>
      <password>${env.GITHUB_TOKEN}</password>
    </server>
  </servers>
</settings>
```

### 3. Keycloak Dependency (One-Time)

Download and cache the Keycloak dependency:
```bash
mkdir -p ~/.m2/repository/org/keycloak
curl https://atlan-public.s3.eu-west-1.amazonaws.com/artifact/keycloak-15.0.2.1.zip -o keycloak-15.0.2.1.zip
unzip -o keycloak-15.0.2.1.zip -d ~/.m2/repository/org
rm keycloak-15.0.2.1.zip
```

### 4. Docker Setup (macOS with Colima)

Install and start Colima:
```bash
brew install colima docker docker-compose
colima start --disk 10 --memory 4
```

## Building Atlas

### Clone and Build

```bash
git clone https://github.com/atlanhq/atlas-metastore.git
cd atlas-metastore
```

**Intel Mac:**
```bash
mvn clean -Dmaven.test.skip -DskipTests -Drat.skip=true -DskipOverlay -DskipEnunciate=true install -Pdist
```

**Apple Silicon Mac:**
```bash
mvn clean -Dos.detected.classifier=osx-x86_64 -Dmaven.test.skip -DskipTests -Drat.skip=true -DskipOverlay -DskipEnunciate=true install -Pdist
```

**Important:** Use `mvn install` (not just `package`) so IntelliJ can resolve local module dependencies.

## Running Dependencies

Start infrastructure services:
```bash
docker-compose -f local-dev/docker-compose.yaml up -d
```

This starts:
- Redis Master (6379) + Slave (6380) + Sentinel (26379)
- Cassandra (9042)
- Elasticsearch (9200)
- Zookeeper (2181)

Wait for services to be healthy before starting Atlas.

### Copy Logback Configuration (Required)

Copy the local logback configuration to disable OpenTelemetry (which requires a collector not available locally):
```bash
cp local-dev/atlas-logback.xml deploy/conf/atlas-logback.xml
```

This disables the OpenTelemetry appender and provides enhanced console output with request tracing.

## Running Atlas

### Option 1: IntelliJ IDEA (Recommended)

Create a Run Configuration:

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
-Dspring.profiles.active=local
```

**Environment Variables:**
```
OTEL_SDK_DISABLED=true
```

> **Important:** The `-Dspring.profiles.active=local` flag is required to load the local Redis service implementation. Without it, you'll get a `NoSuchBeanDefinitionException` for `RedisService`.

**IntelliJ Maven Settings:**
- Go to **Settings → Build → Build Tools → Maven → Runner**
- Add Environment variables: `GITHUB_USERNAME` and `GITHUB_TOKEN`

### Option 2: Command Line

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) OTEL_SDK_DISABLED=true java \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  -Datlas.home=deploy/ \
  -Datlas.conf=deploy/conf \
  -Datlas.data=deploy/data \
  -Datlas.log.dir=deploy/logs \
  -Dembedded.solr.directory=deploy/data \
  -Dzookeeper.snapshot.trust.empty=true \
  -Dlogback.configurationFile=file:./deploy/conf/atlas-logback.xml \
  -Dspring.profiles.active=local \
  -cp "webapp/target/atlas-webapp-3.0.0-SNAPSHOT/WEB-INF/classes:webapp/target/atlas-webapp-3.0.0-SNAPSHOT/WEB-INF/lib/*" \
  org.apache.atlas.Atlas
```

### Access Atlas

- **URL:** http://localhost:21000
- **Username:** `admin`
- **Password:** `admin`

**Note:** The web UI may not load reliably in local development. Use the REST API instead:

```bash
# Verify Atlas is running
curl -u admin:admin http://localhost:21000/api/atlas/v2/types/typedefs/headers

# Example: Get all type definitions
curl -u admin:admin http://localhost:21000/api/atlas/v2/types/typedefs
```

## Troubleshooting

### GitHub 401/403 Errors During Build

- **401 Unauthorized:** GitHub credentials not set. Export `GITHUB_USERNAME` and `GITHUB_TOKEN`
- **403 Forbidden:** Your GitHub account doesn't have access to `atlanhq` organization. Request access from your team.

### Corrupted Maven Cache

If you see `Invalid CEN header` or ZIP errors:
```bash
rm -rf ~/.m2/repository/org/aspectj/aspectjweaver/1.8.9
mvn dependency:resolve -U
```

### Maven Using Wrong Java Version

Check Maven's Java:
```bash
mvn -version
```

If not Java 17, the `.mavenrc` file should fix it. Or manually:
```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn <command>
```

### IntelliJ Cannot Resolve Dependencies

Run `mvn install` (not `package`) to install local modules to your Maven cache:
```bash
mvn clean install -Dmaven.test.skip -DskipTests -Drat.skip=true -Pdist
```

Then in IntelliJ: **Maven → Reload Project**

### Cassandra Tags Keyspace Error

If you see `"errorMessage": "Error fetching all classifications"`:

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

### RedisService Bean Not Found

If you see `NoSuchBeanDefinitionException: No qualifying bean of type 'org.apache.atlas.service.redis.RedisService'`:

This means the Spring profile is not set. Add `-Dspring.profiles.active=local` to your VM options. The local Redis implementation (`RedisServiceLocalImpl`) is only loaded when the `local` profile is active.

### Docker Services Not Starting

Check logs:
```bash
docker-compose -f local-dev/docker-compose.yaml logs -f cassandra
docker-compose -f local-dev/docker-compose.yaml logs -f elasticsearch
```

### Full Environment Reset

```bash
docker-compose -f local-dev/docker-compose.yaml down
docker system prune -a --volumes -f
colima stop
```

## Configuration Files

| File | Purpose |
|------|---------|
| `deploy/conf/atlas-application.properties` | Main Atlas configuration |
| `deploy/conf/users-credentials.properties` | User credentials (SHA-256 hashed) |
| `deploy/conf/atlas-logback.xml` | Logging configuration |
| `local-dev/docker-compose.yaml` | Local infrastructure services (Docker) |
| `local-dev/sentinel.conf` | Redis Sentinel configuration |
| `.mavenrc` | Maven Java version configuration |
| `.java-version` | jenv Java version (17) |

## Notes

- The build command skips tests for faster development builds
- For production builds, remove the skip flags
- Keep your GitHub PAT token secure and never commit it to version control
- Adjust memory and CPU settings in Colima based on your machine's capabilities
- Kafka warnings during startup are normal if Kafka is not running (optional service)
