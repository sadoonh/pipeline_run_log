---
inclusion: auto
name: git-workflow
description: How to commit work and open PRs. Use when the user is finishing a notebook, committing, pushing, or opening a pull request, or working with .ipynb files.
---

# Git Workflow for Analysts

You are helping an analyst who is NOT a git expert. Be safe, explicit, and never
do anything destructive without confirming. When asked to "ship", "commit and
PR", "I'm done", or similar, follow this exact sequence and explain each step in
one short line as you go.

## The "ship it" sequence
1. **Safety check.** Run `git rev-parse --abbrev-ref HEAD`. If on `main`,
   STOP and create a branch first: `analyst/<name>/<short-desc>` (ask for the
   name if unknown). Never commit to `main`.
2. **Show what's changing.** Run `git status` and `git diff --stat`. Briefly
   tell the analyst what will be committed. Flag anything surprising — large
   data files, secrets, or files outside the area they're working in.
3. **Sync with main.** `git fetch origin` then `git rebase origin/main`.
   If there are conflicts, STOP and explain clearly which files conflict and
   offer to walk them through resolving it. Do not guess.
4. **Stage & commit.** Stage the relevant files. Generate a Conventional Commit
   message from the diff (`exp:` for exploratory notebook work, `feat:` for a
   genuinely reusable new notebook/util, `fix:` for a fix). Show the message
   before committing.
5. **Push.** `git push -u origin <branch>`.
6. **Open the PR** against `main` using the GitHub CLI:
   `gh pr create --base main --fill`. Pre-fill the title from the commit.

## Rules
- Never `git push --force` to a shared branch without explicit confirmation.
- Never `git reset --hard` or discard uncommitted work without confirming the
  analyst is OK losing it.
- Keep branches short-lived. If a branch is more than ~5 days old, sync with
  main now rather than letting it drift.
- Keep it calm and short. The analyst should feel like they pressed one button.
