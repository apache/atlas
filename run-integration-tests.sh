#!/bin/bash
# run-integration-tests.sh

set -e

echo "============================================"
echo "Atlas Integration Tests Runner"
echo "============================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color
export JAVA_HOME=`/usr/libexec/java_home -v 17` # Set Java 17 as JAVA_HOME
# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Docker is not running. Please start Docker first.${NC}"
    exit 1
fi

# Parse arguments
SKIP_BUILD=false
KEEP_CONTAINERS=false
DEBUG=false

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --skip-build) SKIP_BUILD=true ;;
        --keep-containers) KEEP_CONTAINERS=true ;;
        --debug) DEBUG=true ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --skip-build      Skip building Atlas WAR and Docker image"
            echo "  --keep-containers Keep containers running after tests"
            echo "  --debug          Enable debug logging"
            echo "  -h, --help       Show this help message"
            exit 0
            ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# Step 1: Build Atlas WAR if not skipping
if [ "$SKIP_BUILD" = false ]; then
    echo -e "${YELLOW}Building Atlas WAR package...${NC}"
    mvn clean -Dos.detected.classifier=osx-x86_64 -Dmaven.test.skip -DskipTests -Drat.skip=true -DskipOverlay -DskipEnunciate=true package -Pdist

    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to build Atlas WAR${NC}"
        exit 1
    fi
    echo -e "${GREEN}Atlas WAR built successfully${NC}"
else
    echo -e "${YELLOW}Skipping Atlas build (--skip-build flag set)${NC}"
fi

# Step 2: Build Docker image
echo -e "${YELLOW}Building Atlas Docker image...${NC}"
docker buildx build --load -t atlanhq/atlas:test .

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to build Docker image${NC}"
    exit 1
fi
echo -e "${GREEN}Docker image built successfully${NC}"

# Step 3: Clean up any existing test containers
echo -e "${YELLOW}Cleaning up existing test containers...${NC}"
docker rm -f atlas-test-zookeeper atlas-test-kafka atlas-test-cassandra \
             atlas-test-elasticsearch atlas-test-redis atlas-test-atlas 2>/dev/null || true

# Step 4: Set test properties
export TESTCONTAINERS_REUSE_ENABLE=true
export TESTCONTAINERS_RYUK_DISABLED=$KEEP_CONTAINERS

if [ "$DEBUG" = true ]; then
    export TESTCONTAINERS_DEBUG=true
    MAVEN_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"
fi

echo "Listing docker images"
docker image ls

# Step 4: Run integration tests
echo -e "${YELLOW}Running integration tests...${NC}"

# Start container log capture in background
mkdir -p target/test-logs
echo "Starting container log monitor..."
bash -c '
    while true; do
        for container_id in $(docker ps -q 2>/dev/null); do
            container_name=$(docker inspect --format="{{.Name}}" "$container_id" 2>/dev/null | sed "s/^\///")
            if [ -n "$container_name" ]; then
                log_file="target/test-logs/${container_name}.log"
                if [ ! -f "${log_file}.capturing" ]; then
                    echo "Capturing logs from: $container_name"
                    touch "${log_file}.capturing"
                    docker logs -f "$container_id" > "$log_file" 2>&1 &
                    echo "$!" >> /tmp/log-capture-pids.txt
                fi
            fi
        done
        sleep 2
    done
' &
MONITOR_PID=$!
echo "Container monitor started (PID: $MONITOR_PID)"

# Run tests
if [ "$DEBUG" = true ]; then
    mvn test -pl webapp -Dtest=AtlasDockerIntegrationTest \
             -Dorg.slf4j.simpleLogger.defaultLogLevel=debug \
             -Dorg.testcontainers.log.level=DEBUG -Dsurefire.useFile=false
else
    mvn test -pl webapp -Dsurefire.useFile=false
fi

TEST_RESULT=$?

# Stop log capture
echo "Stopping container log monitor..."
kill $MONITOR_PID 2>/dev/null || true
if [ -f /tmp/log-capture-pids.txt ]; then
    while read pid; do
        kill "$pid" 2>/dev/null || true
    done < /tmp/log-capture-pids.txt
    rm -f /tmp/log-capture-pids.txt
fi
rm -f target/test-logs/*.capturing

echo "Container logs saved to target/test-logs/"
ls -lh target/test-logs/ 2>/dev/null || true

# Step 6: Collect logs if tests failed
if [ $TEST_RESULT -ne 0 ]; then
    echo -e "${RED}Tests failed! Collecting logs...${NC}"

    mkdir -p target/test-logs

    # Get container logs
    for container in $(docker ps -a --filter "name=atlas-test" --format "{{.Names}}"); do
        echo "Collecting logs from $container..."
        docker logs $container > "target/test-logs/${container}.log" 2>&1
    done

    echo -e "${YELLOW}Logs saved to target/test-logs/${NC}"
fi

# Step 7: Clean up containers if not keeping them
if [ "$KEEP_CONTAINERS" = false ]; then
    echo -e "${YELLOW}Cleaning up test containers...${NC}"
    docker rm -f $(docker ps -a --filter "name=atlas-test" --format "{{.Names}}") 2>/dev/null || true
else
    echo -e "${YELLOW}Keeping containers running (--keep-containers flag set)${NC}"
    echo "You can connect to Atlas at: http://localhost:21000"
    echo "To stop containers manually, run:"
    echo "  docker rm -f \$(docker ps -a --filter 'name=atlas-test' --format '{{.Names}}')"
fi

# Step 8: Report results
if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}============================================${NC}"
    echo -e "${GREEN}Integration tests completed successfully!${NC}"
    echo -e "${GREEN}============================================${NC}"
    exit 0
else
    echo -e "${RED}============================================${NC}"
    echo -e "${RED}Integration tests failed!${NC}"
    echo -e "${RED}Check target/test-logs for details${NC}"
    echo -e "${RED}============================================${NC}"
    exit 1
fi