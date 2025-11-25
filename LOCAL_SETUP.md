# Local Development Setup Guide for Atlas

This guide will help you set up Atlas for local development.

## Prerequisites

### Required Software
- Java 17 (Recommended: Zulu OpenJDK 17)
- Maven 3.8+ 
- Docker (via Colima for macOS)
- Git
- Get the source code from the AtlanHQ repository (An override of Apache Atlas)
- Download the zip and configuration artifacts from https://atlanhq.atlassian.net/wiki/spaces/c873aeb606dd4834a95d9909a757bfa6/pages/800424446/How+to+run+Atlas+on+the+local+machine

### Java Setup
1. Install Java 17:
   ```bash
   brew install zulu17
   ```

2. Set JAVA_HOME:
   ```bash
   export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home
   export PATH=$JAVA_HOME/bin:$PATH
   ```

### Maven Setup

1. Configure GitHub Package Registry access:
   Create or update `~/.m2/settings.xml`:
   ```xml
   <settings>
     <servers>
       <server>
         <id>github</id>
         <username>YOUR_GITHUB_USERNAME</username>
         <password>YOUR_GITHUB_PAT_TOKEN</password>
       </server>
     </servers>
     
     <profiles>
       <profile>
         <id>github</id>
         <repositories>
           <repository>
             <id>github</id>
             <url>https://maven.pkg.github.com/atlanhq/janusgraph</url>
           </repository>
         </repositories>
       </profile>
     </profiles>
     
     <activeProfiles>
       <activeProfile>github</activeProfile>
     </activeProfiles>
   </settings>
   ```

   Note: Generate a GitHub Personal Access Token with `read:packages` scope.

### Docker Setup (using Colima)

1. Install Colima:
   ```bash
   brew install colima
   ```

2. Start Colima:
   ```bash
   colima start
   ```

## Building Atlas

1. Clone the repository:
   ```bash
   git clone https://github.com/atlanhq/atlas-metastore.git
   cd atlas-metastore
   ```

2. Build the project:
   ```bash
   mvn clean -Dos.detected.classifier=osx-x86_64 -Dmaven.test.skip -DskipTests -Drat.skip=true -DskipOverlay -DskipEnunciate=true install package -Pdist
   ```

## Running Dependencies

Atlas requires several services to run. Use Docker Compose to start them:

1. Required Services:
   - Redis (for caching)
   - Cassandra (for metadata storage)
   - Elasticsearch (for search functionality)
   - Kafka (optional - for notifications)

2. Start the services:
   ```bash
   cd deploy
   docker-compose up -d redis cassandra elasticsearch
   ```

   If you need Kafka:
   ```bash
   docker-compose up -d kafka
   ```

3. Wait for services to be healthy:
   - Redis: Default port 6379
   - Cassandra: Default port 9042
   - Elasticsearch: Default port 9200
   - Kafka (if enabled): Default port 9092

## Running Atlas

1. Start Atlas server:
   ```bash
   java -Datlas.home=deploy/ -Datlas.conf=deploy/conf -Datlas.data=deploy/data -Datlas.log.dir=deploy/logs -Dranger.plugin.atlas.policy.pollIntervalMs=300000 -Dembedded.solr.directory=deploy/data -Dlogback.configurationFile=file:./deploy/conf/atlas-logback.xml -Dzookeeper.snapshot.trust.empty=true --add-opens java.base/java.lang=ALL-UNNAMED -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dorg.apache.http.nio.reactor.ioThreadCount=4 -Dcassandra.connection.pool.max=4 -Djanusgraph.connection.pool.max=2 -Dnetty.eventLoopThreads=4 -XX:+UseCompressedOops -XX:+UseCompressedClassPointers -Xms512m
   org.apache.atlas.Atlas
   ```

2. Access the UI:
   - URL: http://localhost:21000
   - Default credentials: admin/admin

## Troubleshooting

1. If services fail to start, check Docker logs:
   ```bash
   docker-compose logs -f [service_name]
   ```

2. For Atlas server issues, check logs in:
   ```
   logs/application.log
   ```

3. Common issues:
   - Port conflicts: Ensure no other services are using required ports
   - Memory issues: Adjust Docker resource limits in Colima
   - Connection timeouts: Ensure all required services are healthy before starting Atlas

## Additional Resources

For more detailed information, refer to:
- [Atlas Documentation](https://atlas.apache.org/documentation.html)
- [Internal Setup Guide](https://atlanhq.atlassian.net/wiki/spaces/c873aeb606dd4834a95d9909a757bfa6/pages/800424446/How+to+run+Atlas+on+the+local+machine)

## Notes

- The build command skips tests and various checks for faster development builds
- For production builds, remove the skip flags
- Keep your GitHub PAT token secure and never commit it to version control
- Adjust memory and CPU settings in Colima based on your machine's capabilities