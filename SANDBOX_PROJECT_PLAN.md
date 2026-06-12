# Sandbox Job Framework Project Plan

## 1. Project Purpose

The goal of this project is to create a lightweight internal sandbox job framework that allows users to write Python transformation jobs and publish the results into a shared AWS-based sandbox.

Users should be able to:

- Create a new sandbox job from the command line.
- Write Python logic that reads data from cataloged sources.
- Transform data using pandas or similar Python tools.
- Write results to the sandbox as queryable tables.
- Run recurring jobs on a daily schedule.
- Run one-time jobs once after they are added.
- Track what jobs ran, when they ran, and whether they succeeded.

The project is not intended to be a heavy orchestration platform. It is meant to be a simple, practical framework for controlled sandbox data work.

---

## 2. What the Project Is Achieving

This project gives analysts and data users a repeatable way to create sandbox tables without manually managing S3 paths, Glue table registration, Athena setup, or job scheduling.

At a high level, the project supports this workflow:

```text
User writes Python job
        ↓
Job reads source data
        ↓
Job transforms data
        ↓
Job writes output to sandbox
        ↓
GitHub Actions runs the job
        ↓
Logs are recorded
        ↓
Sandbox table is available for querying
```

The project provides structure around user-written Python jobs while still keeping the user experience simple.

---

## 3. High-Level Design

The project has four main parts:

```text
1. Sandbox API
2. Sandbox Jobs
3. Sandbox Platform
4. GitHub Actions
```

### Sandbox API

The sandbox API contains helper functions for reading and writing sandbox data.

It hides the lower-level AWS details from users.

Example responsibilities:

- Read data using Athena.
- Write pandas DataFrames to S3 as Parquet.
- Register or update tables in Glue.
- List sandbox tables.
- Delete sandbox tables when needed.

### Sandbox Jobs

Sandbox jobs are user-created Python files.

There are two job types:

```text
daily/
```

Jobs that run every day.

```text
one_time/
```

Jobs that run once and are skipped after they succeed.

Each job includes a small metadata contract and a `main(ctx)` function.

### Sandbox Platform

The sandbox platform contains the framework code that supports the jobs.

Example responsibilities:

- CLI command for creating new jobs.
- Job discovery.
- Job validation.
- Running daily jobs.
- Running one-time jobs.
- Logging job results.
- Tracking one-time job completion.

### GitHub Actions

GitHub Actions provides the automation layer.

Example responsibilities:

- Validate jobs on pull requests.
- Run daily jobs on a schedule.
- Run one-time jobs when new one-time jobs are merged.
- Allow manual job runs when needed.

---

## 4. Expected Repository Structure

The project lives under the `sandbox/` package.

```text
repo/
  pyproject.toml
  README.md

  sandbox/
    __init__.py

    sandbox_api/
      __init__.py
      io.py

    sandbox_platform/
      __init__.py
      cli.py
      context.py
      runner.py
      validate_jobs.py
      logging.py
      state.py

    sandbox_jobs/
      __init__.py

      daily/
        __init__.py

      one_time/
        __init__.py

      _shared/
        __init__.py
```

GitHub Actions workflows will live here:

```text
.github/
  workflows/
    validate-sandbox-jobs.yml
    run-daily-sandbox-jobs.yml
    run-one-time-sandbox-jobs.yml
```

---

## 5. User Workflow

### Creating a New Job

A user creates a new job with:

```bash
uv run sandbox job init
```

The CLI asks for:

```text
JOB_ID
JOB_TYPE
OWNER
OUTPUT_TABLES
DESCRIPTION
```

Example input:

```text
JOB_ID: 2026_06_10_backfill_customer_history
JOB_TYPE [daily/one_time]: one_time
OWNER: analytics
OUTPUT_TABLES comma-separated: analytics__customer_history_backfill
DESCRIPTION: One-time backfill of 2024 customer order history.
```

The CLI creates this file:

```text
sandbox/sandbox_jobs/one_time/2026_06_10_backfill_customer_history.py
```

### Editing the Job

The generated job file contains a starter template.

The user replaces the placeholder logic with their transformation code.

### Submitting the Job

The user opens a pull request.

The validation workflow checks that the job has the required metadata and basic structure.

### Running the Job

After the job is merged:

- Daily jobs run on the daily schedule.
- One-time jobs run once and then get marked as complete.

---

## 6. Job Contract

Each job file must include a small metadata block.

Example:

```python
JOB_ID = "analytics_customer_summary"
JOB_TYPE = "daily"
OWNER = "analytics"
OUTPUT_TABLES = ["analytics__customer_summary"]
DESCRIPTION = "Daily customer summary sandbox table."


def main(ctx) -> None:
    ...
```

The metadata lets the platform understand:

- What the job is called.
- Whether it is daily or one-time.
- Who owns it.
- Which sandbox table or tables it writes.
- What the job is meant to do.

The `main(ctx)` function is where the user writes the actual job logic.

Users are still responsible for calling:

```python
write_df_to_sandbox(...)
```

inside the job.

---

## 7. Job Types

### Daily Jobs

Daily jobs are recurring jobs.

They live in:

```text
sandbox/sandbox_jobs/daily/
```

They should be safe to rerun.

Common examples:

- Daily customer summary.
- Latest inventory snapshot.
- Daily project metrics.
- Refreshed reporting extracts.

### One-Time Jobs

One-time jobs are jobs that should run once.

They live in:

```text
sandbox/sandbox_jobs/one_time/
```

Common examples:

- Historical backfills.
- One-off cleanup jobs.
- Temporary rebuilds.
- Migration support jobs.

After a one-time job succeeds, the framework records a success marker so the job does not run again automatically.

---

## 8. Sandbox API Design

The sandbox API should provide simple user-facing functions.

Initial functions:

```python
write_df_to_sandbox(...)
read_df_from_sandbox(...)
read_table_from_sandbox(...)
list_sandbox_tables(...)
delete_table_from_sandbox(...)
```

The most important function is:

```python
write_df_to_sandbox(df, table_name, if_exists="replace")
```

It should:

- Validate the table name.
- Write the DataFrame to S3 as Parquet.
- Register or update the Glue table.
- Make the table queryable from Athena.
- Log basic write information when running as part of a job.

The API should hide AWS implementation details from users as much as possible.

---

## 9. Runtime Context

Each job receives a simple context object.

Example fields:

```text
ctx.job_id
ctx.job_type
ctx.owner
ctx.run_id
ctx.run_date
ctx.dry_run
ctx.commit_sha
ctx.github_run_id
ctx.github_actor
```

This allows jobs to use runtime information without hardcoding it.

Example:

```python
def main(ctx) -> None:
    print(ctx.run_date)
```

The context should stay simple. It is only there to pass useful runtime metadata into jobs.

---

## 10. Logging Design

The project should log job activity so users can answer:

- Did the job run?
- Did it succeed?
- When did it run?
- Who owns it?
- What table did it write?
- What GitHub run produced it?

For the first version, logs can be written as JSON files to S3.

Suggested S3 structure:

```text
s3://<sandbox-bucket>/sandbox-platform/logs/job_runs/
s3://<sandbox-bucket>/sandbox-platform/logs/table_writes/
s3://<sandbox-bucket>/sandbox-platform/state/one_time_success/
```

### Job Run Logs

A job run log records one job attempt.

It should include:

```text
run_id
job_id
job_type
owner
status
started_at
finished_at
duration_seconds
run_date
output_tables
commit_sha
github_run_id
github_actor
error_message
```

### Table Write Logs

A table write log records when a job writes a sandbox table.

It should include:

```text
run_id
job_id
table_name
row_count
column_count
columns
s3_path
if_exists
written_at
dry_run
```

### One-Time Success Markers

A one-time success marker records that a one-time job already succeeded.

The runner checks this before running one-time jobs.

---

## 11. GitHub Actions Design

The project should use three GitHub Actions workflows.

### Validate Sandbox Jobs

Runs on pull requests.

Purpose:

- Check Python formatting and linting.
- Validate job metadata.
- Confirm `main(ctx)` exists.
- Confirm `JOB_TYPE` matches the folder.
- Confirm output table names are valid.

### Run Daily Sandbox Jobs

Runs on a daily cron schedule.

Purpose:

- Discover jobs in `sandbox_jobs/daily/`.
- Run each daily job.
- Continue running remaining jobs if one fails.
- Log success or failure.
- Fail the workflow overall if any job fails.

### Run One-Time Sandbox Jobs

Runs when files under `sandbox_jobs/one_time/` change on `main`.

Purpose:

- Discover jobs in `sandbox_jobs/one_time/`.
- Skip jobs with an existing success marker.
- Run pending one-time jobs.
- Create a success marker after successful completion.
- Log success or failure.

---

## 12. AWS Design

The project uses AWS services in a lightweight way.

### S3

Used for:

- Sandbox table storage.
- Athena query output.
- Job logs.
- One-time job success markers.

### Glue Data Catalog

Used for:

- Registering sandbox tables.
- Making S3 Parquet datasets discoverable.

### Athena

Used for:

- Querying cataloged source data.
- Querying sandbox tables.

### IAM

GitHub Actions should assume an AWS role with only the required permissions.

The role needs access to:

- Read source data.
- Write sandbox table data.
- Write Athena query results.
- Read and write Glue metadata for the sandbox database.
- Write job logs and state markers.

---

## 13. Implementation Phases

### Phase 1: Package and CLI

Goal: make the command work.

Deliverables:

- Project packaging works with uv.
- `sandbox job init` runs.
- CLI prompts for job metadata.
- CLI creates files in the correct job folder.

Success criteria:

```bash
uv run sandbox job init
```

creates a valid starter job file.

---

### Phase 2: Basic Job Validation

Goal: catch simple mistakes before merge.

Deliverables:

- Validation script.
- Pull request workflow.
- Checks for required metadata.
- Checks for `main(ctx)`.
- Checks for valid output table names.

Success criteria:

A pull request fails if a job does not follow the required structure.

---

### Phase 3: Daily Job Runner

Goal: run recurring jobs.

Deliverables:

- Job discovery for `daily/`.
- Runtime context object.
- Daily runner.
- Basic job run logging.
- GitHub Actions scheduled workflow.

Success criteria:

Daily jobs can run from GitHub Actions and produce logs.

---

### Phase 4: One-Time Job Runner

Goal: run one-time jobs once.

Deliverables:

- Job discovery for `one_time/`.
- One-time success marker logic.
- One-time runner.
- GitHub Actions workflow for one-time jobs.

Success criteria:

A one-time job runs once after merge and is skipped on future runs.

---

### Phase 5: Table Write Logging

Goal: track sandbox writes.

Deliverables:

- Add logging inside `write_df_to_sandbox`.
- Record row counts, columns, table names, and output paths.

Success criteria:

Every sandbox table write produces a table write log.

---

### Phase 6: Cleanup and Documentation

Goal: make the project usable by someone else.

Deliverables:

- README updates.
- Example daily job.
- Example one-time job.
- Notes about reruns and one-time jobs.
- Notes about required environment variables.

Success criteria:

A new user can read the README, create a job, and understand how it runs.

---

## 14. Initial Non-Goals

To keep the project simple, do not build these in the first version:

- Airflow-style dependencies.
- Complex job registry.
- Per-job dependency environments.
- Web UI.
- Advanced permissions by user.
- Complex retries.
- Automatic backfill scheduler.
- Full data quality framework.
- Multi-environment promotion system.

These can be added later if the project needs them.

---

## 15. Basic Rules for Users

Users should follow these rules:

1. Put recurring jobs in `sandbox_jobs/daily/`.
2. Put one-time jobs in `sandbox_jobs/one_time/`.
3. Use `sandbox job init` to create new jobs.
4. Keep all execution logic inside `main(ctx)`.
5. Do not run queries or writes at import time.
6. Use `write_df_to_sandbox()` for sandbox outputs.
7. Make daily jobs safe to rerun.
8. Use a new `JOB_ID` for a new one-time job.
9. Do not have two jobs write to the same table unless intentionally approved.
10. Keep sandbox jobs small enough for the GitHub Actions runner.

---

## 16. Success Criteria for the Project

The project is successful when:

- A user can create a job with `sandbox job init`.
- The job file is created in the right folder.
- The job follows a consistent metadata structure.
- Pull requests catch basic job structure issues.
- Daily jobs run automatically.
- One-time jobs run once and are skipped after success.
- Sandbox table writes are logged.
- A user can understand what ran and what table was produced.

---

## 17. Simple End State

The final simple end state should feel like this:

```text
1. User creates a job.
2. User fills in the transformation.
3. User opens a PR.
4. Validation checks the job.
5. Job is merged.
6. GitHub Actions runs it.
7. Output table appears in the sandbox.
8. Logs show what happened.
```

That is the core project.
