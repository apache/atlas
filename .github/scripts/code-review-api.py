"""
PR Code Reviewer API Client for GitHub Actions.

This script calls the mothership pr_code_reviewer agent via the LangGraph Platform API.
It sends PR metadata and waits for the agent to complete and post a review comment.

Usage:
    Set environment variables and run:
    $ python code-review-api.py

Environment Variables Required:
    - PR_URL: Full URL of the Pull Request
    - PR_TITLE: Title of the Pull Request
    - PR_DESCRIPTION: Description/body of the Pull Request (optional)
    - REPO_URL: Repository URL
    - SOURCE_BRANCH: Source branch name
    - TARGET_BRANCH: Target branch name
"""

import sys
import aiohttp
import asyncio
import os
import time
from typing import Any, Dict

# Mothership API endpoint (synchronous - waits for result)
MOTHERSHIP_API_ENDPOINT = "https://mothership-beta.atlan.dev/runs/wait"
MOTHERSHIP_ASSISTANT_SEARCH_ENDPOINT = "https://mothership-beta.atlan.dev/assistants/search"

# Default timeout for the API call (30 minutes - PR review can take time)
API_TIMEOUT_SECONDS = 1800


async def verify_network_connectivity():
    """Verify network connectivity to mothership API"""
    print("Verifying network connectivity...")

    try:
        async with aiohttp.ClientSession() as session:
            print("\nTesting mothership-beta.atlan.dev connectivity...")
            start_time = time.time()
            try:
                async with session.get(
                    'https://mothership-beta.atlan.dev/docs',
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    duration = time.time() - start_time
                    print(f"mothership-beta.atlan.dev/docs - Status: {response.status}, Time: {duration:.2f}s")
                    if response.status == 200:
                        print("Mothership API is accessible")
                        return True
                    else:
                        print(f"Mothership returned status {response.status}")
                        return False
            except asyncio.TimeoutError:
                duration = time.time() - start_time
                print(f"mothership-beta.atlan.dev - Timeout after {duration:.2f}s")
                return False
            except Exception as e:
                duration = time.time() - start_time
                print(f"mothership-beta.atlan.dev - Error: {str(e)} (after {duration:.2f}s)")
                return False

    except Exception as e:
        print(f"Network check failed: {str(e)}")
        return False


async def fetch_assistant_id() -> str:
    """
    Fetch the assistant_id for pr_code_reviewer from mothership API.
    """
    print("Fetching assistant ID for pr_code_reviewer...")

    payload = {
        "metadata": {},
        "graph_id": "pr_code_reviewer",
        "name": "",
        "limit": 10,
        "offset": 0,
        "sort_by": "assistant_id",
        "sort_order": "asc",
        "select": ["assistant_id"]
    }

    headers = {"Content-Type": "application/json"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                MOTHERSHIP_ASSISTANT_SEARCH_ENDPOINT,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status != 200:
                    text = await response.text()
                    print(f"Failed to fetch assistant ID: {response.status} - {text}")
                    return None

                data = await response.json()
                if data and isinstance(data, list) and len(data) > 0:
                    assistant_id = data[0].get("assistant_id")
                    print(f"Found assistant ID: {assistant_id}")
                    return assistant_id
                else:
                    print(f"No assistant found in response: {data}")
                    return None

    except Exception as e:
        print(f"Error fetching assistant ID: {str(e)}")
        return None


def get_pr_metadata() -> Dict[str, Any]:
    """Get PR metadata from environment variables"""
    pr_url = os.getenv('PR_URL', '')
    pr_title = os.getenv('PR_TITLE', '')
    pr_description = os.getenv('PR_DESCRIPTION', '')
    repo_url = os.getenv('REPO_URL', '')
    source_branch = os.getenv('SOURCE_BRANCH', '')
    target_branch = os.getenv('TARGET_BRANCH', 'master')

    # Validate required fields
    missing = []
    if not pr_url:
        missing.append('PR_URL')
    if not pr_title:
        missing.append('PR_TITLE')
    if not repo_url:
        missing.append('REPO_URL')
    if not source_branch:
        missing.append('SOURCE_BRANCH')

    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    return {
        "pr_url": pr_url,
        "pr_title": pr_title,
        "pr_description": pr_description or "",
        "repo_url": repo_url,
        "source_branch": source_branch,
        "target_branch": target_branch,
    }


async def trigger_pr_review(pr_metadata: Dict[str, Any], assistant_id: str) -> Dict[str, Any]:
    """
    Trigger the PR code reviewer agent via mothership API.

    This calls the synchronous endpoint which waits for the agent to complete.
    The agent will:
    1. Clone the repo
    2. Get the PR diff
    3. Run triage, context retrieval, and assessment
    4. Post a review comment to the GitHub PR
    """
    if not assistant_id:
        print("No assistant_id provided")
        return {"success": False, "status_code": 0, "response": "No assistant_id", "duration": 0}

    # Build the API payload
    payload = {
        "assistant_id": assistant_id,
        "input": pr_metadata
    }

    headers = {
        "Content-Type": "application/json"
    }

    print("\n" + "=" * 60)
    print("Triggering PR Code Review")
    print("=" * 60)
    print(f"PR URL: {pr_metadata['pr_url']}")
    print(f"PR Title: {pr_metadata['pr_title']}")
    print(f"Branch: {pr_metadata['source_branch']} -> {pr_metadata['target_branch']}")
    print(f"Assistant ID: {assistant_id}")
    print("=" * 60 + "\n")

    try:
        start_time = time.time()

        async with aiohttp.ClientSession() as session:
            print("Sending request to mothership API...")
            print(f"   Endpoint: {MOTHERSHIP_API_ENDPOINT}")
            print(f"   Timeout: {API_TIMEOUT_SECONDS}s")

            async with session.post(
                MOTHERSHIP_API_ENDPOINT,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=API_TIMEOUT_SECONDS)
            ) as response:
                duration = time.time() - start_time
                response_json = await response.json()

                print(f"\nResponse received in {duration:.2f}s")
                print(f"   Status Code: {response.status}")

                print("\nResponse Body:")
                print("-" * 40)
                import json
                print(json.dumps(response_json, indent=2, default=str)[:2000])
                print("-" * 40)

                # Check for agent-level errors in the response
                if isinstance(response_json, dict):
                    if response_json.get("status") == "error":
                        print(f"Agent failed with error: {response_json.get('error')}")
                        return {
                            "success": False,
                            "status_code": response.status,
                            "response": response_json,
                            "duration": duration
                        }

                    # Check for github_post_results to confirm comment was posted
                    final_state = response_json.get("output", response_json)
                    github_results = final_state.get("github_post_results", {})
                    if github_results:
                        if github_results.get("success"):
                            print(f"Comment posted: {github_results.get('comment_url')}")
                        else:
                            print(f"GitHub post failed: {github_results.get('error')}")

                if 200 <= response.status < 1800:
                    print("PR Code Review completed successfully!")
                    return {
                        "success": True,
                        "status_code": response.status,
                        "response": response_json,
                        "duration": duration
                    }
                else:
                    print(f"PR Code Review failed with status {response.status}")
                    print(f"   Response: {response_json}")
                    return {
                        "success": False,
                        "status_code": response.status,
                        "response": response_json,
                        "duration": duration
                    }

    except asyncio.TimeoutError:
        duration = time.time() - start_time
        print(f"Request timed out after {duration:.2f}s")
        return {
            "success": False,
            "status_code": 0,
            "response": "Request timeout",
            "duration": duration
        }

    except Exception as e:
        duration = time.time() - start_time
        print(f"Error: {str(e)}")
        return {
            "success": False,
            "status_code": 0,
            "response": str(e),
            "duration": duration
        }


async def main():
    print("\n" + "=" * 60)
    print("PR CODE REVIEWER - GitHub Action Client")
    print("=" * 60 + "\n")

    # Step 1: Verify network connectivity
    if not await verify_network_connectivity():
        print("\nCannot reach mothership API. Check VPN connection.")
        sys.exit(1)

    # Step 2: Get PR metadata from environment
    print("\nGetting PR metadata from environment...")
    pr_metadata = get_pr_metadata()

    # Step 3: Fetch Assistant ID
    assistant_id = await fetch_assistant_id()
    if not assistant_id:
        print("Could not fetch assistant ID. Aborting.")
        sys.exit(1)

    # Step 4: Trigger the PR review
    result = await trigger_pr_review(pr_metadata, assistant_id)

    # Step 5: Print summary and exit
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Success: {result['success']}")
    print(f"Duration: {result['duration']:.2f}s")
    print(f"Status Code: {result['status_code']}")
    print("=" * 60 + "\n")

    if not result["success"]:
        print("PR Code Review failed")
        sys.exit(1)
    else:
        print("PR Code Review completed - check the PR for the review comment!")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
