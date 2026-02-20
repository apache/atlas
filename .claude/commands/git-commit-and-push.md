---
description: Commit all staged/unstaged code changes and push to the current remote branch
allowed-tools: [Bash]
argument-hint: "[optional commit message]"
---

# Git Commit and Push

Commit all code changes and push to the current remote branch in one step.

## Input

- `$ARGUMENTS` (optional): A commit message. If not provided, auto-generate one from the diff.

## Steps

### 1. Gather context

Run these in parallel:

```bash
git status
```

```bash
git diff --stat
```

```bash
git diff --staged --stat
```

```bash
git log --oneline -5
```

```bash
git branch --show-current
```

### 2. Stage changes

Stage all modified and new files that are part of the current work. Use specific file paths — do NOT use `git add -A` or `git add .`. Skip files that look like secrets (`.env`, credentials, etc.) and warn the user if any are detected.

If there are no changes to commit, inform the user and stop.

### 3. Generate commit message

If `$ARGUMENTS` is provided, use it as the commit message.

Otherwise, analyze the diff and generate a conventional commit message:
- Use the `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:` prefix as appropriate
- First line: concise summary under 72 characters
- Optional body: explain the "why" if the change is non-trivial
- Always append the co-author trailer

### 4. Commit

```bash
git commit -m "$(cat <<'EOF'
<commit message here>
EOF
)"
```

### 5. Push

Push to the current branch's remote tracking branch:

```bash
git push origin $(git branch --show-current)
```

If the branch has no upstream, use:

```bash
git push -u origin $(git branch --show-current)
```

### 6. Confirm

Print a short summary:
- Branch name
- Commit hash (short)
- Commit message (first line)
- Files changed count
- Push status (success/failure)
