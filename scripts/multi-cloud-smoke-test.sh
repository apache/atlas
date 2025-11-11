#!/bin/bash

##############################################################################
# Multi-Cloud Smoke Test Script
# 
# Tests Atlas statefulset across multiple cloud environments in parallel
# 
# Usage:
#   ./multi-cloud-smoke-test.sh <test-image>
# 
# Example:
#   ./multi-cloud-smoke-test.sh ghcr.io/atlanhq/atlas-metastore:latest
#
# Prerequisites:
#   - kubectl configured with access to vclusters
#   - kubeconfig-aws.yaml, kubeconfig-azure.yaml, and kubeconfig-gcp.yaml in current directory
#   - jq installed
##############################################################################

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check arguments
if [ $# -ne 1 ]; then
  echo -e "${RED}Error: Missing test image argument${NC}"
  echo "Usage: $0 <test-image>"
  exit 1
fi

TEST_IMAGE=$1

echo -e "${BLUE}=================================================="
echo -e "STARTING PARALLEL SMOKE TESTS"
echo -e "==================================================${NC}"
echo "Test Image: $TEST_IMAGE"
echo ""

# Create logs directory
mkdir -p smoke-test-logs

# Define test function
test_cloud() {
  CLOUD=$1
  KUBECONFIG_FILE=$2
  LOG_FILE="smoke-test-logs/${CLOUD}.log"
  
  {
    echo "=========================================="
    echo "[${CLOUD}] Starting smoke test"
    echo "=========================================="
    echo "Image: $TEST_IMAGE"
    echo "Kubeconfig: $KUBECONFIG_FILE"
    echo ""
    
    # Verify kubeconfig exists
    if [ ! -f "$KUBECONFIG_FILE" ]; then
      echo "[${CLOUD}] ❌ ERROR: Kubeconfig not found: $KUBECONFIG_FILE"
      exit 1
    fi
    
    # Patch statefulset
    echo "[${CLOUD}] Patching Atlas statefulset..."
    if ! KUBECONFIG=$KUBECONFIG_FILE kubectl set image statefulset/atlas \
      atlas-main=$TEST_IMAGE \
      -n atlas; then
      echo "[${CLOUD}] ❌ ERROR: Failed to patch statefulset"
      exit 1
    fi
    echo "[${CLOUD}] ✓ StatefulSet patched"
    echo ""
    
    # Wait for rollout
    echo "[${CLOUD}] Waiting for rollout (10 min timeout)..."
    if KUBECONFIG=$KUBECONFIG_FILE kubectl rollout status statefulset/atlas -n atlas --timeout=10m; then
      echo "[${CLOUD}] ✓ Rollout completed successfully"
    else
      echo "[${CLOUD}] ❌ ERROR: Rollout failed or timed out"
      echo "[${CLOUD}] Pod status:"
      KUBECONFIG=$KUBECONFIG_FILE kubectl get pods -n atlas -l app=atlas
      echo "[${CLOUD}] Recent events (excluding Normal):"
      KUBECONFIG=$KUBECONFIG_FILE kubectl get events -n atlas --sort-by='.lastTimestamp' | grep -v "Normal" | tail -20
      exit 1
    fi
    echo ""
    
    # Port-forward and test
    echo "[${CLOUD}] Setting up port-forward..."
    # Use unique port per cloud to avoid conflicts
    if [ "$CLOUD" = "AWS" ]; then
      LOCAL_PORT=21001
    elif [ "$CLOUD" = "Azure" ]; then
      LOCAL_PORT=21002
    else
      LOCAL_PORT=21003
    fi
    
    # Check if service exists
    if ! KUBECONFIG=$KUBECONFIG_FILE kubectl get svc atlas-service-atlas -n atlas &>/dev/null; then
      echo "[${CLOUD}] ❌ ERROR: Service atlas-service-atlas not found"
      KUBECONFIG=$KUBECONFIG_FILE kubectl get svc -n atlas
      exit 1
    fi
    
    KUBECONFIG=$KUBECONFIG_FILE kubectl port-forward -n atlas svc/atlas-service-atlas $LOCAL_PORT:80 > /tmp/pf-${CLOUD}.log 2>&1 &
    PF_PID=$!
    
    # Wait for port-forward to be ready (with timeout)
    echo "[${CLOUD}] Waiting for port-forward to be ready..."
    for i in {1..30}; do
      if lsof -i :$LOCAL_PORT &>/dev/null; then
        echo "[${CLOUD}] ✓ Port-forward is ready"
        break
      fi
      if [ $i -eq 30 ]; then
        echo "[${CLOUD}] ❌ ERROR: Port-forward failed to start"
        echo "[${CLOUD}] Port-forward logs:"
        cat /tmp/pf-${CLOUD}.log 2>/dev/null || echo "No logs available"
        kill $PF_PID 2>/dev/null || true
        exit 1
      fi
      sleep 1
    done
    
    # Status check with retries
    echo "[${CLOUD}] Running status check..."
    MAX_RETRIES=5
    for attempt in $(seq 1 $MAX_RETRIES); do
      STATUS_RESPONSE=$(curl -f -s "http://localhost:$LOCAL_PORT/api/atlas/admin/status" 2>&1)
      CURL_EXIT=$?
      
      if [ $CURL_EXIT -eq 0 ]; then
        STATUS=$(echo "$STATUS_RESPONSE" | jq -r '.Status' 2>/dev/null)
        if [ "$STATUS" = "ACTIVE" ]; then
          echo "[${CLOUD}] ✓ Atlas is ACTIVE"
          break
        else
          echo "[${CLOUD}] ⚠ Status: $STATUS (attempt $attempt/$MAX_RETRIES)"
        fi
      else
        echo "[${CLOUD}] ⚠ Curl failed with code $CURL_EXIT (attempt $attempt/$MAX_RETRIES)"
        echo "[${CLOUD}] Response: $STATUS_RESPONSE"
      fi
      
      if [ $attempt -eq $MAX_RETRIES ]; then
        echo "[${CLOUD}] ❌ ERROR: Could not reach endpoint after $MAX_RETRIES attempts"
        echo "[${CLOUD}] Port-forward logs:"
        cat /tmp/pf-${CLOUD}.log 2>/dev/null || echo "No logs available"
        kill $PF_PID 2>/dev/null || true
        exit 1
      fi
      
      sleep 5
    done
    
    # Cleanup
    kill $PF_PID 2>/dev/null || true
    rm -f /tmp/pf-${CLOUD}.log
    
    echo ""
    echo "[${CLOUD}] ✅✅✅ SMOKE TEST PASSED ✅✅✅"
    echo ""
  } > "$LOG_FILE" 2>&1
}

# Export variables for subshells
export TEST_IMAGE
export -f test_cloud

# Start tests in parallel
echo -e "${YELLOW}Launching AWS test...${NC}"
bash -c "test_cloud AWS kubeconfig-aws.yaml" &
PID_AWS=$!

echo -e "${YELLOW}Launching Azure test...${NC}"
bash -c "test_cloud Azure kubeconfig-azure.yaml" &
PID_AZURE=$!

echo -e "${YELLOW}Launching GCP test...${NC}"
bash -c "test_cloud GCP kubeconfig-gcp.yaml" &
PID_GCP=$!

echo ""
echo -e "${BLUE}All tests running in parallel...${NC}"
echo "AWS PID: $PID_AWS"
echo "Azure PID: $PID_AZURE"
echo "GCP PID: $PID_GCP"
echo ""

# Tail logs in real-time (interleaved) with color coding
tail -f smoke-test-logs/AWS.log 2>/dev/null | while IFS= read -r line; do
  if echo "$line" | grep -q "ERROR\|❌\|failed"; then
    echo -e "${RED}[AWS] $line${NC}"
  elif echo "$line" | grep -q "✓\|✅\|PASSED\|successfully"; then
    echo -e "${GREEN}[AWS] $line${NC}"
  else
    echo "[AWS] $line"
  fi
done &
TAIL_AWS=$!

tail -f smoke-test-logs/Azure.log 2>/dev/null | while IFS= read -r line; do
  if echo "$line" | grep -q "ERROR\|❌\|failed"; then
    echo -e "${RED}[Azure] $line${NC}"
  elif echo "$line" | grep -q "✓\|✅\|PASSED\|successfully"; then
    echo -e "${GREEN}[Azure] $line${NC}"
  else
    echo "[Azure] $line"
  fi
done &
TAIL_AZURE=$!

tail -f smoke-test-logs/GCP.log 2>/dev/null | while IFS= read -r line; do
  if echo "$line" | grep -q "ERROR\|❌\|failed"; then
    echo -e "${RED}[GCP] $line${NC}"
  elif echo "$line" | grep -q "✓\|✅\|PASSED\|successfully"; then
    echo -e "${GREEN}[GCP] $line${NC}"
  else
    echo "[GCP] $line"
  fi
done &
TAIL_GCP=$!

# Wait for tests to complete
FAILED=0

if wait $PID_AWS; then
  echo -e "${GREEN}✓ AWS test completed successfully${NC}"
else
  echo -e "${RED}✗ AWS test failed${NC}"
  FAILED=1
fi

if wait $PID_AZURE; then
  echo -e "${GREEN}✓ Azure test completed successfully${NC}"
else
  echo -e "${RED}✗ Azure test failed${NC}"
  FAILED=1
fi

if wait $PID_GCP; then
  echo -e "${GREEN}✓ GCP test completed successfully${NC}"
else
  echo -e "${RED}✗ GCP test failed${NC}"
  FAILED=1
fi

# Stop tailing logs
kill $TAIL_AWS $TAIL_AZURE $TAIL_GCP 2>/dev/null || true

# Show final summary
echo ""
echo -e "${BLUE}=================================================="
echo -e "SMOKE TEST RESULTS"
echo -e "==================================================${NC}"

echo -e "${YELLOW}AWS Results:${NC}"
if grep -q "SMOKE TEST PASSED" smoke-test-logs/AWS.log; then
  cat smoke-test-logs/AWS.log | tail -5 | while IFS= read -r line; do
    if echo "$line" | grep -q "PASSED"; then
      echo -e "${GREEN}$line${NC}"
    else
      echo "$line"
    fi
  done
else
  cat smoke-test-logs/AWS.log | tail -5 | while IFS= read -r line; do
    echo -e "${RED}$line${NC}"
  done
fi

echo ""
echo -e "${YELLOW}Azure Results:${NC}"
if grep -q "SMOKE TEST PASSED" smoke-test-logs/Azure.log; then
  cat smoke-test-logs/Azure.log | tail -5 | while IFS= read -r line; do
    if echo "$line" | grep -q "PASSED"; then
      echo -e "${GREEN}$line${NC}"
    else
      echo "$line"
    fi
  done
else
  cat smoke-test-logs/Azure.log | tail -5 | while IFS= read -r line; do
    echo -e "${RED}$line${NC}"
  done
fi

echo ""
echo -e "${YELLOW}GCP Results:${NC}"
if grep -q "SMOKE TEST PASSED" smoke-test-logs/GCP.log; then
  cat smoke-test-logs/GCP.log | tail -5 | while IFS= read -r line; do
    if echo "$line" | grep -q "PASSED"; then
      echo -e "${GREEN}$line${NC}"
    else
      echo "$line"
    fi
  done
else
  cat smoke-test-logs/GCP.log | tail -5 | while IFS= read -r line; do
    echo -e "${RED}$line${NC}"
  done
fi

echo -e "${BLUE}==================================================${NC}"

# Exit with failure if any test failed
if [ $FAILED -eq 1 ]; then
  echo -e "${RED}❌ ERROR: One or more smoke tests failed${NC}"
  exit 1
fi

echo -e "${GREEN}✅ All smoke tests passed!${NC}"
