#!/bin/bash
# check-prerequisites.sh - Validates local development prerequisites for Atlas Metastore

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "============================================"
echo "Atlas Metastore Prerequisites Check"
echo "============================================"
echo ""

ERRORS=0
WARNINGS=0

check_pass() {
    echo -e "${GREEN}✓${NC} $1"
}

check_fail() {
    echo -e "${RED}✗${NC} $1"
    ERRORS=$((ERRORS + 1))
}

check_warn() {
    echo -e "${YELLOW}!${NC} $1"
    WARNINGS=$((WARNINGS + 1))
}

# 1. Check Java 17
echo "Checking Java..."
if command -v java &> /dev/null; then
    JAVA_VERSION=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$JAVA_VERSION" = "17" ]; then
        check_pass "Java 17 installed"
    else
        check_fail "Java 17 required (found: Java $JAVA_VERSION)"
        echo "       Install with: brew install openjdk@17"
    fi
else
    check_fail "Java not found"
    echo "       Install with: brew install openjdk@17"
fi

# 2. Check Maven
echo ""
echo "Checking Maven..."
if command -v mvn &> /dev/null; then
    MVN_VERSION=$(mvn -version 2>&1 | head -1 | awk '{print $3}')
    check_pass "Maven installed (version $MVN_VERSION)"

    # Check Maven's Java version
    MVN_JAVA=$(mvn -version 2>&1 | grep "Java version" | awk -F'[, ]' '{print $3}' | cut -d'.' -f1)
    if [ "$MVN_JAVA" = "17" ]; then
        check_pass "Maven using Java 17"
    else
        check_warn "Maven using Java $MVN_JAVA (should be 17)"
        echo "       Create .mavenrc or set JAVA_HOME before running mvn"
    fi
else
    check_fail "Maven not found"
    echo "       Install with: brew install maven"
fi

# 3. Check Docker
echo ""
echo "Checking Docker..."
if command -v docker &> /dev/null; then
    if docker info &> /dev/null; then
        check_pass "Docker is running"
    else
        check_fail "Docker installed but not running"
        echo "       Start with: colima start --disk 10 --memory 4"
    fi
else
    check_fail "Docker not found"
    echo "       Install with: brew install docker colima"
fi

# 4. Check GitHub credentials
echo ""
echo "Checking GitHub credentials..."
if [ -n "$GITHUB_USERNAME" ] && [ -n "$GITHUB_TOKEN" ]; then
    check_pass "GitHub credentials set (GITHUB_USERNAME, GITHUB_TOKEN)"
else
    check_warn "GitHub credentials not set in environment"
    echo "       Set: export GITHUB_USERNAME=<your-username>"
    echo "       Set: export GITHUB_TOKEN=<your-pat-token>"
    echo "       Token needs 'read:packages' scope and atlanhq org access"
fi

# Check settings.xml
if [ -f ~/.m2/settings.xml ]; then
    if grep -q "github" ~/.m2/settings.xml 2>/dev/null; then
        check_pass "Maven settings.xml has GitHub server configured"
    else
        check_warn "Maven settings.xml missing GitHub server configuration"
    fi
else
    check_fail "Maven settings.xml not found"
    echo "       Create ~/.m2/settings.xml with GitHub credentials"
fi

# 5. Check Keycloak dependency
echo ""
echo "Checking Keycloak dependency..."
if [ -d ~/.m2/repository/org/keycloak/keycloak-core ]; then
    check_pass "Keycloak dependency cached"
else
    check_fail "Keycloak dependency not cached"
    echo "       Run:"
    echo "       mkdir -p ~/.m2/repository/org/keycloak"
    echo "       curl https://atlan-public.s3.eu-west-1.amazonaws.com/artifact/keycloak-15.0.2.1.zip -o keycloak.zip"
    echo "       unzip -o keycloak.zip -d ~/.m2/repository/org"
fi

# 6. Check project files
echo ""
echo "Checking project files..."
if [ -d "$PROJECT_ROOT/deploy" ]; then
    check_pass "deploy/ directory exists"
else
    check_fail "deploy/ directory missing"
    echo "       Extract deploy.zip to project root"
fi

if [ -f "$PROJECT_ROOT/local-dev/docker-compose.yaml" ]; then
    check_pass "local-dev/docker-compose.yaml exists"
else
    check_fail "local-dev/docker-compose.yaml missing"
fi

if [ -f "$PROJECT_ROOT/deploy/conf/atlas-application.properties" ]; then
    check_pass "atlas-application.properties exists"
else
    check_fail "atlas-application.properties missing"
fi

if [ -f "$PROJECT_ROOT/deploy/conf/atlas-logback.xml" ]; then
    check_pass "atlas-logback.xml exists (enhanced logging)"
else
    check_warn "atlas-logback.xml missing (optional but recommended)"
    echo "       Copy with: cp local-dev/atlas-logback.xml deploy/conf/"
fi

# 7. Check Docker services (if Docker is running)
echo ""
echo "Checking Docker services..."
if docker info &> /dev/null; then
    for service in cassandra elasticsearch redis; do
        if docker ps --format '{{.Names}}' | grep -q "$service"; then
            check_pass "$service container running"
        else
            check_warn "$service container not running"
            echo "       Start with: docker-compose -f local-dev/docker-compose.yaml up -d"
        fi
    done
fi

# Summary
echo ""
echo "============================================"
if [ $ERRORS -eq 0 ] && [ $WARNINGS -eq 0 ]; then
    echo -e "${GREEN}All checks passed! Ready to build.${NC}"
elif [ $ERRORS -eq 0 ]; then
    echo -e "${YELLOW}$WARNINGS warning(s) found. Review above.${NC}"
else
    echo -e "${RED}$ERRORS error(s), $WARNINGS warning(s) found.${NC}"
    echo "Fix errors before building."
    exit 1
fi
echo "============================================"
