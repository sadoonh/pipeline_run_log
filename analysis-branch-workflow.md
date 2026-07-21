# Analysis Branch Workflow

Use a separate `analysis/*` branch for each piece of analysis work.

The goal is to keep every branch focused on one task so that changes are easier to review and less likely to cause drift or merge conflicts.

## 1. Start from the latest `main`

Before creating a branch, update your local `main` branch:

```bash
git switch main
git pull origin main
```

## 2. Create an analysis branch

Create a new branch using the `analysis/` prefix and a short description of the work:

```bash
git switch -c analysis/<topic>
```

Example:

```bash
git switch -c analysis/customer-retention
```

## 3. Work on one thing only

Each branch should contain changes for one specific analysis task.

Do not combine unrelated analyses, fixes, or cleanup work in the same branch. A focused branch:

- Is easier to review
- Reduces merge conflicts
- Prevents the branch from drifting away from `main`
- Makes it easier to understand and revert changes

When a new or unrelated task appears, create another branch from the latest `main`.

## 4. Commit and push the branch

Commit your work with a clear message:

```bash
git add .
git commit -m "Complete customer retention analysis"
git push -u origin analysis/customer-retention
```

## 5. Open a pull request into `main`

After the analysis is complete, open a pull request from the `analysis/*` branch into `main`.

The pull request should clearly explain:

- What was analyzed
- What changed
- The main findings or outcome
- Any follow-up work required

## 6. Merge and close the branch

After the pull request is reviewed and approved, merge it into `main`.

Delete the analysis branch after it is merged. Do not reuse an old analysis branch for a new task.

## Summary

The expected workflow is:

```text
Update main
    ↓
Create analysis/<topic>
    ↓
Work on one analysis task
    ↓
Push the branch
    ↓
Open a PR into main
    ↓
Review and merge
    ↓
Delete the branch
```
