CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                  BIGSERIAL                   NOT NULL,
    run_id              BIGINT                      NOT NULL,
    workflow_name       VARCHAR(255)                NOT NULL,
    conclusion          VARCHAR(50),
    run_started_at      TIMESTAMPTZ                 NOT NULL,
    run_completed_at    TIMESTAMPTZ,
    duration_seconds    INT GENERATED ALWAYS AS (
                            EXTRACT(EPOCH FROM (run_completed_at - run_started_at))::INT
                        ) STORED,
    repository          VARCHAR(255)                NOT NULL,
    branch              VARCHAR(255),
    actor               VARCHAR(100),
    head_sha            VARCHAR(40),
    run_url             VARCHAR(500),
    failed_step         VARCHAR(255),
    source              VARCHAR(20)                 NOT NULL DEFAULT 'live',
    ingestion_timestamp TIMESTAMPTZ                 NOT NULL DEFAULT now(),

    CONSTRAINT pipeline_runs_pkey
        PRIMARY KEY (id),
    CONSTRAINT pipeline_runs_run_id_unique
        UNIQUE (run_id),

    CONSTRAINT pipeline_runs_conclusion_check
        CHECK (conclusion IN (
            'success', 'failure', 'cancelled',
            'skipped', 'timed_out', 'action_required', 'neutral', 'stale'
        ))
); 

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at
    ON pipeline_runs (run_started_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_repo_workflow
    ON pipeline_runs (repository, workflow_name);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_conclusion
    ON pipeline_runs (conclusion)
    WHERE conclusion = 'failure';

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_run_id
    ON pipeline_runs (run_id);
