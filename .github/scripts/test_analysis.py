#!/usr/bin/env python3
"""
Test Analysis Script using LiteLLM Proxy
Analyzes test results after integration tests complete
"""

import os
import sys
import json
import argparse
import requests
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Any, List, Tuple


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
    
    def chat(self, messages: List[Dict[str, str]], max_tokens: int = 3000, temperature: float = 0.1) -> str:
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
            raise Exception(f"LiteLLM API error: {response.status_code} - {response.text}")
        
        result = response.json()
        return result["choices"][0]["message"]["content"]


class TestResultParser:
    """Parse JUnit XML test results"""
    
    @staticmethod
    def parse_junit_xml(xml_path: str) -> Tuple[int, int, int, int, List[str]]:
        """Parse JUnit XML and extract test counts and failures"""
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        total = int(root.attrib.get('tests', 0))
        failures = int(root.attrib.get('failures', 0))
        errors = int(root.attrib.get('errors', 0))
        skipped = int(root.attrib.get('skipped', 0))
        
        failed_tests = []
        for testcase in root.findall('.//testcase'):
            name = testcase.attrib.get('name', 'Unknown')
            classname = testcase.attrib.get('classname', '')
            
            failure = testcase.find('failure')
            error = testcase.find('error')
            
            if failure is not None or error is not None:
                message = failure.attrib.get('message', '') if failure is not None else error.attrib.get('message', '')
                failed_tests.append(f"{classname}.{name}: {message}")
        
        return total, failures + errors, skipped, total - failures - errors - skipped, failed_tests


def analyze_tests(
    llm_client: LiteLLMClient,
    pr_number: int,
    pr_title: str,
    test_status: str,
    total: int,
    passed: int,
    failed: int,
    skipped: int,
    failed_tests: List[str],
    pr_diff: str
):
    """Analyze test results and provide insights"""
    
    print(f"📊 Test Results: {passed} passed, {failed} failed, {skipped} skipped (total: {total})")
    
    if failed == 0:
        analysis_type = "coverage"
        focus = "Analyze test coverage for changed code and identify any gaps."
    else:
        analysis_type = "failures"
        focus = "Analyze why tests failed and provide concrete fix suggestions."
    
    prompt = f"""You are analyzing integration test results for a PR in Apache Atlas Metastore.

## PR Information
- **PR Number:** #{pr_number}
- **Title:** {pr_title}
- **Test Status:** {test_status}

## Test Results
- **Total Tests:** {total}
- **Passed:** {passed}
- **Failed:** {failed}
- **Skipped:** {skipped}

## Failed Tests
{chr(10).join(failed_tests) if failed_tests else "None"}

## PR Changes (Diff)
```diff
{pr_diff[:10000]}
```

## Your Task
{focus}

Provide analysis in this format:

### Test Results Summary
[Brief overview of test status]

### {'Root Cause Analysis' if failed > 0 else 'Coverage Assessment'}
[Detailed analysis relating test results to PR changes]

### {'Recommended Fixes' if failed > 0 else 'Coverage Recommendations'}
[Concrete suggestions with code examples if applicable]

### Conclusion
[Final assessment and next steps]
"""
    
    messages = [{"role": "user", "content": prompt}]
    
    print("🤖 Analyzing test results with Claude...")
    try:
        analysis = llm_client.chat(messages, max_tokens=3000, temperature=0.1)
        return analysis
    except Exception as e:
        print(f"❌ Failed to analyze tests: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Test Analysis via LiteLLM")
    parser.add_argument("--pr-number", type=int, required=True)
    parser.add_argument("--pr-title", required=True)
    parser.add_argument("--test-status", required=True)
    parser.add_argument("--junit-xml", required=True, help="Path to JUnit XML file")
    parser.add_argument("--pr-diff", required=True, help="PR diff content")
    parser.add_argument("--litellm-url", default="https://llmproxy.atlan.dev/v1")
    parser.add_argument("--litellm-key", required=True)
    parser.add_argument("--model", default="claude")
    parser.add_argument("--output", default="-", help="Output file (- for stdout)")
    
    args = parser.parse_args()
    
    # Parse test results
    print(f"📋 Parsing test results from {args.junit_xml}...")
    try:
        total, failed, skipped, passed, failed_tests = TestResultParser.parse_junit_xml(args.junit_xml)
    except Exception as e:
        print(f"❌ Failed to parse test results: {e}")
        sys.exit(1)
    
    # Initialize LLM client
    llm_client = LiteLLMClient(args.litellm_url, args.litellm_key, args.model)
    
    # Analyze
    analysis = analyze_tests(
        llm_client,
        args.pr_number,
        args.pr_title,
        args.test_status,
        total,
        passed,
        failed,
        skipped,
        failed_tests,
        args.pr_diff
    )
    
    if analysis:
        output_content = f"""## 🧪 Integration Test Analysis

{analysis}

---
*Powered by Claude via LiteLLM Proxy*
"""
        
        if args.output == "-":
            print(output_content)
        else:
            with open(args.output, 'w') as f:
                f.write(output_content)
            print(f"✅ Analysis written to {args.output}")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
