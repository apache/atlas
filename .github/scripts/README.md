# Claude PR Review with LiteLLM Proxy

This directory contains scripts for AI-powered PR review using your internal LiteLLM proxy instead of direct Anthropic API.

## Overview

**Migration from:** `anthropics/claude-code-action@v1` (direct Anthropic API)  
**Migration to:** Custom Python scripts using LiteLLM proxy at `https://llmproxy.atlan.dev`

## Files

### Scripts

- **`claude_review.py`** - Main PR review script
  - Fetches PR diff and changed files
  - Sends to Claude via LiteLLM proxy
  - Posts review comment on PR
  
- **`test_analysis.py`** - Test result analyzer
  - Parses JUnit XML test results
  - Analyzes failures and coverage gaps
  - Posts analysis comment on PR
  
- **`requirements.txt`** - Python dependencies
  - `requests` for HTTP calls

### Workflows

- **`claude-litellm.yml`** - GitHub Actions workflow
  - Replaces the original `claude.yml`
  - Uses LiteLLM proxy instead of Anthropic API

## Setup

### 1. Add GitHub Secret

The workflow requires a `LITELLM_API_KEY` secret:

```bash
# Using GitHub CLI
gh secret set LITELLM_API_KEY --repo atlanhq/atlas-metastore

# Or via GitHub UI:
# https://github.com/atlanhq/atlas-metastore/settings/secrets/actions
```

**Value:** Your LiteLLM API key (e.g., `sk-W6qr_YpPtzp7Cv510IPCDQ`)

### 2. Configure Model Name

The scripts use `--model "claude"` by default. Update this in the workflow if your LiteLLM proxy uses a different model name:

```yaml
--model "claude-3-5-sonnet"  # or whatever your proxy supports
```

To check available models:
```bash
curl https://llmproxy.atlan.dev/v1/models \
  -H "Authorization: Bearer YOUR_API_KEY" | jq -r '.data[].id'
```

### 3. Enable the New Workflow

Two options:

**Option A: Keep Both (Recommended for Testing)**
- Rename old: `claude.yml` → `claude-anthropic.yml.disabled`
- Keep new: `claude-litellm.yml`
- Test the new workflow on a few PRs
- Delete old workflow once confident

**Option B: Replace Directly**
- Delete `claude.yml`
- Rename `claude-litellm.yml` → `claude.yml`

## Usage

### Automatic PR Review

When a PR is opened or updated:
1. Workflow triggers automatically
2. Python script fetches PR diff
3. Sends to Claude via LiteLLM proxy
4. Posts review comment on PR

### Test Analysis

After integration tests complete:
1. Workflow downloads test artifacts
2. Parses JUnit XML results
3. Sends to Claude for analysis
4. Posts analysis comment on PR

## Testing Locally

### Test PR Review Script

```bash
cd .github/scripts

python3 claude_review.py \
  --pr-number 6083 \
  --pr-title "Test PR" \
  --pr-author "hitk6" \
  --repo "atlanhq/atlas-metastore" \
  --litellm-url "https://llmproxy.atlan.dev/v1" \
  --litellm-key "sk-W6qr_YpPtzp7Cv510IPCDQ" \
  --github-token "$GITHUB_TOKEN" \
  --model "claude"
```

### Test Analysis Script

```bash
python3 test_analysis.py \
  --pr-number 6083 \
  --pr-title "Test PR" \
  --test-status "passed" \
  --junit-xml "path/to/TEST-results.xml" \
  --pr-diff "$(gh pr diff 6083)" \
  --litellm-url "https://llmproxy.atlan.dev/v1" \
  --litellm-key "sk-W6qr_YpPtzp7Cv510IPCDQ" \
  --model "claude" \
  --output "-"  # stdout
```

## Differences from Original Workflow

| Feature | Original (`claude.yml`) | New (`claude-litellm.yml`) |
|---------|------------------------|---------------------------|
| API Endpoint | Anthropic direct | LiteLLM proxy |
| Authentication | `ANTHROPIC_API_KEY` | `LITELLM_API_KEY` |
| Interactive `@claude` | ✅ Supported | ❌ Not implemented yet* |
| Auto PR review | ✅ | ✅ |
| Test analysis | ✅ | ✅ |
| QA Pipeline | ✅ | ❌ Not implemented yet* |
| Inline comments | ✅ | ✅ (in summary comment) |

*Can be added if needed

## Cost Comparison

**Original (Anthropic Direct):**
- ~$0.50-$1.00 per PR review
- ~$0.30 per test analysis
- Billed to Anthropic account

**New (LiteLLM Proxy):**
- Internal infrastructure cost
- No direct API charges
- Usage tracked via proxy metrics

## Troubleshooting

### Error: "No healthy deployments for this model"

Your LiteLLM proxy doesn't have the specified model configured.

**Fix:**
1. Check available models:
   ```bash
   curl https://llmproxy.atlan.dev/v1/models \
     -H "Authorization: Bearer YOUR_KEY"
   ```
2. Update `--model` parameter in workflow to match available model

### Error: "Failed to get PR diff"

GitHub CLI (`gh`) might not be installed or authenticated.

**Fix:** The workflow uses `gh` which is pre-installed in GitHub Actions runners.

### Script hangs or times out

LiteLLM proxy might be slow or overloaded.

**Fix:**
- Check proxy status
- Increase timeout in script: `timeout=300` (5 minutes)

## Migration Checklist

- [ ] Add `LITELLM_API_KEY` secret to repository
- [ ] Verify model name with LiteLLM admin
- [ ] Test locally with sample PR
- [ ] Enable `claude-litellm.yml` workflow
- [ ] Monitor first few PR reviews
- [ ] Disable/delete old `claude.yml` workflow
- [ ] Update team documentation

## Support

**LiteLLM Proxy Issues:** Contact your infrastructure team  
**Script Issues:** Create an issue in atlas-metastore repo  
**Workflow Issues:** Check GitHub Actions logs

## Future Enhancements

Possible additions:
- [ ] Interactive `@claude` mentions support
- [ ] Full QA pipeline (`/claude-qa-pipeline` command)
- [ ] Inline review comments (currently all in one comment)
- [ ] Streaming responses for faster feedback
- [ ] Retry logic for proxy failures
- [ ] Cost/usage tracking metrics
