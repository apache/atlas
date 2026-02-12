#!/usr/bin/env python3
"""
Claude PR Review Script using LiteLLM Proxy
Replacement for anthropics/claude-code-action@v1
"""

import os
import sys
import json
import argparse
import requests
from typing import Optional, Dict, Any, List


class LiteLLMClient:
    """Client for LiteLLM proxy API"""
    
    def __init__(self, base_url: str, api_key: str, model: str = "claude"):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.model = model
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def chat(self, messages: List[Dict[str, str]], max_tokens: int = 4000, temperature: float = 0.1) -> str:
        """Send chat completion request to LiteLLM proxy"""
        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature
        }
        
        response = requests.post(
            f"{self.base_url}/chat/completions",
            headers=self.headers,
            json=payload,
            timeout=120
        )
        
        if response.status_code != 200:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            raise Exception(f"LiteLLM API error: {response.status_code} - {response.text}")
        
        result = response.json()
        return result["choices"][0]["message"]["content"]


class GitHubAPI:
    """GitHub API client for PR operations"""
    
    def __init__(self, token: str, repo: str):
        self.token = token
        self.repo = repo
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }
        self.base_url = "https://api.github.com"
    
    def get_pr_diff(self, pr_number: int) -> str:
        """Get PR diff"""
        import subprocess
        result = subprocess.run(
            ["gh", "pr", "diff", str(pr_number)],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout
    
    def get_pr_files(self, pr_number: int) -> List[Dict[str, Any]]:
        """Get list of changed files in PR"""
        url = f"{self.base_url}/repos/{self.repo}/pulls/{pr_number}/files"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def post_pr_comment(self, pr_number: int, body: str):
        """Post a comment on the PR"""
        url = f"{self.base_url}/repos/{self.repo}/issues/{pr_number}/comments"
        response = requests.post(url, headers=self.headers, json={"body": body})
        response.raise_for_status()
        return response.json()
    
    def post_review_comment(self, pr_number: int, body: str, commit_id: str, path: str, line: int):
        """Post an inline review comment"""
        url = f"{self.base_url}/repos/{self.repo}/pulls/{pr_number}/comments"
        payload = {
            "body": body,
            "commit_id": commit_id,
            "path": path,
            "line": line
        }
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()


def load_review_prompt() -> str:
    """Load the review prompt template"""
    return """You are reviewing a PR for Apache Atlas Metastore — a Java 17 / Maven project
that implements a metadata catalog built on JanusGraph, Cassandra, Elasticsearch, and Kafka.

## Project Context
- Core business logic lives in `repository/` module
- Entity preprocessors in `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/preprocessor/`
- REST API layer in `webapp/`
- API models and client libraries in `intg/`
- Helm charts in `helm/`
- CI/CD workflows in `.github/workflows/`

## Review Focus Areas

1. **Java Code Quality**
   - Thread safety (Atlas is multi-threaded)
   - Proper null checks and error handling
   - Resource cleanup (try-with-resources for streams, graph transactions)
   - Consistent use of existing patterns (PreProcessor pattern, store pattern)

2. **Graph Database Concerns**
   - JanusGraph transaction handling (commit/rollback)
   - Vertex/Edge property access patterns
   - Index usage and query efficiency

3. **Security**
   - Authorization checks in REST endpoints
   - Input validation (especially qualifiedName, GUID parameters)
   - No secrets or credentials in code

4. **Performance**
   - N+1 query patterns in graph traversals
   - Unnecessary object creation in hot paths
   - Elasticsearch query efficiency
   - Proper use of caching

5. **Backward Compatibility**
   - REST API contract changes
   - TypeDef changes that could break existing entities
   - Kafka message format changes

## Review Rules — IMPORTANT
- ONLY leave comments for: critical bugs, security vulnerabilities, unexpected runtime issues (NPE, resource leaks, race conditions), or concrete improvement suggestions with code examples
- Every comment MUST reference the specific code and explain WHY it's a problem — not just describe WHAT the code does
- NEVER leave positive/affirmation comments like "Good change", "Nice refactoring" — these are noise
- NEVER comment on trivial style, formatting, or naming unless it introduces a real bug
- If you have a suggestion, include a concrete code snippet showing the improvement
- Provide a brief overall summary at the end
- If the PR is clean with no real issues, just post a short summary — NO detailed comments needed
- Fewer high-quality comments >> many low-value comments

## Output Format
Provide your review in this format:

### Summary
[Brief 2-3 sentence overview of the PR and overall code quality]

### Critical Issues
[List any critical bugs, security issues, or runtime problems. If none, write "None identified."]

### Suggestions
[List concrete improvement suggestions with code examples. If none, write "No suggestions."]

### Conclusion
[Final recommendation: APPROVE / APPROVE WITH COMMENTS / REQUEST CHANGES]
"""


def review_pr(
    llm_client: LiteLLMClient,
    github_api: GitHubAPI,
    pr_number: int,
    pr_title: str,
    pr_author: str
):
    """Main PR review function"""
    
    print(f"📋 Reviewing PR #{pr_number}: {pr_title}")
    print(f"👤 Author: {pr_author}")
    
    # Get PR diff
    print("📥 Fetching PR diff...")
    try:
        diff = github_api.get_pr_diff(pr_number)
    except Exception as e:
        print(f"❌ Failed to get PR diff: {e}")
        sys.exit(1)
    
    if len(diff) > 50000:
        diff = diff[:50000] + "\n\n[... diff truncated due to size ...]"
        print("⚠️  Diff truncated due to size")
    
    # Get changed files
    print("📁 Fetching changed files...")
    try:
        files = github_api.get_pr_files(pr_number)
        file_list = [f["filename"] for f in files]
        print(f"📂 Changed files ({len(file_list)}): {', '.join(file_list[:10])}")
        if len(file_list) > 10:
            print(f"   ... and {len(file_list) - 10} more")
    except Exception as e:
        print(f"⚠️  Could not fetch files: {e}")
        file_list = []
    
    # Prepare review prompt
    review_prompt = load_review_prompt()
    
    messages = [
        {
            "role": "user",
            "content": f"""{review_prompt}

## PR Information
- **PR Number:** #{pr_number}
- **Title:** {pr_title}
- **Author:** {pr_author}
- **Changed Files:** {', '.join(file_list) if file_list else 'N/A'}

## PR Diff
```diff
{diff}
```

Please review this PR and provide your analysis following the format specified above.
"""
        }
    ]
    
    # Call LLM
    print("🤖 Analyzing PR with Claude via LiteLLM...")
    try:
        review_content = llm_client.chat(messages, max_tokens=4000, temperature=0.1)
    except Exception as e:
        print(f"❌ Failed to get review from LLM: {e}")
        sys.exit(1)
    
    # Post review comment
    print("💬 Posting review comment...")
    try:
        comment_body = f"""## 🤖 Claude AI Review

{review_content}

---
*Powered by Claude via LiteLLM Proxy*
"""
        github_api.post_pr_comment(pr_number, comment_body)
        print("✅ Review posted successfully!")
    except Exception as e:
        print(f"❌ Failed to post comment: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Claude PR Review via LiteLLM")
    parser.add_argument("--pr-number", type=int, required=True, help="PR number to review")
    parser.add_argument("--pr-title", required=True, help="PR title")
    parser.add_argument("--pr-author", required=True, help="PR author")
    parser.add_argument("--repo", required=True, help="GitHub repository (owner/repo)")
    parser.add_argument("--litellm-url", default="https://llmproxy.atlan.dev/v1", help="LiteLLM base URL")
    parser.add_argument("--litellm-key", required=True, help="LiteLLM API key")
    parser.add_argument("--github-token", required=True, help="GitHub token")
    parser.add_argument("--model", default="claude", help="Model name in LiteLLM")
    
    args = parser.parse_args()
    
    # Initialize clients
    llm_client = LiteLLMClient(args.litellm_url, args.litellm_key, args.model)
    github_api = GitHubAPI(args.github_token, args.repo)
    
    # Run review
    review_pr(
        llm_client,
        github_api,
        args.pr_number,
        args.pr_title,
        args.pr_author
    )


if __name__ == "__main__":
    main()
