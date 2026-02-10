---
description: Perform Root Cause Analysis on a Linear ticket
argument-hint: <TICKET-ID>
allowed-tools: [Bash, Read, Grep, Glob, Task, Edit, Write, AskUserQuestion, EnterPlanMode]
---

# Root Cause Analysis for Linear Ticket

Ticket ID: $ARGUMENTS

## Phase 1: Fetch Ticket Details

First, extract the team key and issue number from the ticket ID.
- Example: `MS-508` → team = `MS`, number = `508`

Parse the ticket ID from $ARGUMENTS to extract these values.

Get the Linear API key from `.claude/config.json`:
```bash
cat .claude/config.json | grep -o '"LINEAR_API_KEY": "[^"]*"' | cut -d'"' -f4
```

Then fetch the ticket using Linear GraphQL API:
```bash
curl -s -X POST https://api.linear.app/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: API_KEY_HERE" \
  -d '{"query": "{ issues(filter: { number: { eq: NUMBER }, team: { key: { eq: \"TEAM\" } } }) { nodes { id identifier title description state { name } assignee { name } priority labels { nodes { name } } comments { nodes { body user { name } createdAt } } } } }"}'
```

## Phase 2: Analyze Ticket Information

From the ticket response, extract and note:
- **Error messages or codes** mentioned in description or comments
- **Affected components/services** - what part of the system is impacted
- **Symptoms** - what the user observed (timeouts, failures, errors)
- **Stack traces or logs** - if any are attached or mentioned
- **Timeline** - when did it start, any correlating events

## Phase 3: Parallel Codebase Exploration

Launch 2-3 Task agents with `subagent_type: Explore` **IN PARALLEL** (single message, multiple tool calls) to investigate different aspects:

**Agent 1 - Error/Exception Analysis:**
Prompt: "Search the codebase for [error message/exception from ticket]. Find where this error is thrown, what conditions trigger it, and how it's handled. Report file paths with line numbers."

**Agent 2 - Code Flow Analysis:**
Prompt: "Trace the code flow for [affected functionality from ticket]. Find the entry points, key methods, and data transformations. Identify any bottlenecks or problematic patterns. Report file paths with line numbers."

**Agent 3 - Similar Pattern Analysis:**
Prompt: "Search for how similar problems are handled elsewhere in the codebase. For example, if this is about bulk operations timing out, find how other bulk operations (like tag propagation) handle this. Report the patterns found."

## Phase 4: Synthesize Root Cause

After all exploration agents complete, combine their findings to identify:

1. **Direct Cause** - The immediate technical reason for the failure
2. **Why it happened** - The architectural or design gap that allowed this
3. **Contributing factors** - What made the problem worse

## Phase 5: Generate RCA Document

Create a brief, easy-to-understand RCA in this exact format:

```markdown
## RCA: [TICKET-ID] - [Title]

### Summary
[2-3 sentence description of the issue in plain language]

---

### What Happened
- **First symptom:** [What the user/system observed]
- **Underlying issue:** [Technical cause in simple terms]

---

### Root Cause
[Clear explanation WITHOUT code - explain what went wrong and why in plain language that anyone can understand]

---

### Possible Solutions
| Approach | Description |
|----------|-------------|
| **Short-term unblock** | [Quick fix or workaround] |
| **Long-term solution** | [Proper fix following existing patterns] |

---

### Classification
[Bug / Optimization / Architecture issue] - brief justification

---

### Immediate Action
[What action is needed now, or "No action required" with reason]
```

## Phase 6: Review and Post

1. Display the complete RCA document to the user
2. Ask: "Ready to post this RCA as a comment on Linear ticket [TICKET-ID]? (yes/no)"
3. Wait for user confirmation
4. If approved, post using Linear GraphQL API:

```bash
cat << 'JSONEOF' | curl -s -X POST https://api.linear.app/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: API_KEY_HERE" \
  -d @-
{
  "query": "mutation CreateComment($input: CommentCreateInput!) { commentCreate(input: $input) { success comment { id } } }",
  "variables": {
    "input": {
      "issueId": "ISSUE_UUID_FROM_FETCH",
      "body": "RCA_CONTENT_HERE"
    }
  }
}
JSONEOF
```

Use heredoc (as shown above) to properly handle multiline RCA content with special characters.

## Phase 7: Implementation (Optional)

After the RCA is posted (or if the user declines posting), offer to implement one of the proposed solutions.

1. Use AskUserQuestion to ask:
   - Question: "Would you like me to implement one of the proposed solutions?"
   - Options:
     - "Short-term fix" - Implement the quick fix/workaround
     - "Long-term solution" - Implement the proper fix
     - "No, just the RCA" - End here

2. If the user selects an implementation option:
   - Use EnterPlanMode to design the implementation approach
   - In plan mode:
     - Identify the specific files that need changes
     - Outline the code modifications required
     - Note any tests that need to be added/updated
     - Consider backward compatibility
   - After plan approval, implement the changes using Edit/Write tools
   - Run the build command to verify: `JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home /opt/homebrew/bin/mvn compile -pl repository -am -DskipTests -Drat.skip=true`

3. After implementation:
   - Summarize what was changed
   - Ask if the user wants to commit the changes

## Important Notes

- Always show the draft RCA before posting
- Keep the RCA concise - focus on what matters
- Avoid code snippets in the final RCA (use plain language)
- Include file:line references only in the exploration phase, not in final RCA
- Compare with existing patterns (like how tags handle similar issues)
- Implementation is optional - respect if the user only wants analysis
- Always enter plan mode before implementing to get user approval on the approach
