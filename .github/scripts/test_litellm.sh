#!/bin/bash
# Test script to validate LiteLLM proxy connection and available models

set -e

LITELLM_URL="${LITELLM_URL:-https://llmproxy.atlan.dev/v1}"
LITELLM_KEY="${LITELLM_KEY:-}"

if [ -z "$LITELLM_KEY" ]; then
    echo "❌ Error: LITELLM_KEY environment variable not set"
    echo ""
    echo "Usage: LITELLM_KEY=your-key ./test_litellm.sh"
    exit 1
fi

echo "🔍 Testing LiteLLM Proxy Connection"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "URL: $LITELLM_URL"
echo ""

# Test 1: List models
echo "1️⃣  Fetching available models..."
MODELS=$(curl -s "$LITELLM_URL/models" \
    -H "Authorization: Bearer $LITELLM_KEY" \
    -H "Content-Type: application/json")

if echo "$MODELS" | jq -e '.data' > /dev/null 2>&1; then
    echo "✅ Successfully connected to LiteLLM proxy"
    echo ""
    echo "📋 Available models:"
    echo "$MODELS" | jq -r '.data[].id' | while read model; do
        echo "   - $model"
    done
    echo ""
else
    echo "❌ Failed to fetch models"
    echo "Response: $MODELS"
    exit 1
fi

# Test 2: Test chat completion with first available model
FIRST_MODEL=$(echo "$MODELS" | jq -r '.data[0].id')
echo "2️⃣  Testing chat completion with model: $FIRST_MODEL"

CHAT_RESPONSE=$(curl -s "$LITELLM_URL/chat/completions" \
    -H "Authorization: Bearer $LITELLM_KEY" \
    -H "Content-Type: application/json" \
    -d "{
        \"model\": \"$FIRST_MODEL\",
        \"messages\": [{\"role\": \"user\", \"content\": \"Say 'Hello, Atlas Metastore!' and identify yourself\"}],
        \"max_tokens\": 100,
        \"temperature\": 0.1
    }")

if echo "$CHAT_RESPONSE" | jq -e '.choices[0].message.content' > /dev/null 2>&1; then
    echo "✅ Chat completion successful!"
    echo ""
    echo "🤖 Response:"
    echo "$CHAT_RESPONSE" | jq -r '.choices[0].message.content'
    echo ""
else
    echo "❌ Chat completion failed"
    echo "Response: $CHAT_RESPONSE"
    exit 1
fi

# Summary
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ All tests passed!"
echo ""
echo "📝 Next steps:"
echo "   1. Add LITELLM_API_KEY secret to GitHub repository"
echo "   2. Update workflow model parameter if needed (currently 'claude')"
echo "   3. Enable claude-litellm.yml workflow"
echo ""
echo "Recommended model for workflow: $FIRST_MODEL"
