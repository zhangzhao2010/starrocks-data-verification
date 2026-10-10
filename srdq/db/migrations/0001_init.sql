-- srdq v1.3 schema: 32 tables in schema srdq (fresh schema, no v1.2 compatibility).
CREATE SCHEMA IF NOT EXISTS srdq;
SET search_path = srdq;

CREATE SEQUENCE srdq.ready_seq;

-- ---------------------------------------------------------------- global control
CREATE TABLE dq_control_state (
    id                          int PRIMARY KEY CHECK (id = 1),
    control_generation          uuid NOT NULL,
    status                      text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'RECOVERY')),
    queue_revision              bigint NOT NULL DEFAULT 1 CHECK (queue_revision > 0),
    queue_materialized_revision bigint NOT NULL DEFAULT 1 CHECK (queue_materialized_revision > 0),
    window_state                text NOT NULL DEFAULT 'CLOSED' CHECK (window_state IN ('OPEN', 'DRAINING', 'CLOSED')),
    window_generation           bigint NOT NULL DEFAULT 1,
    window_key                  text NULL,
    window_opened_at            timestamptz NULL,
    window_closed_at            timestamptz NULL,
    window_override             jsonb NOT NULL DEFAULT '{}'::jsonb,
    cluster_state               jsonb NOT NULL DEFAULT '{}'::jsonb,
    transfer_approval_ref       text NULL,
    last_report_window_key      text NULL,
    last_purge_date             date NULL,
    unknown_periods             jsonb NOT NULL DEFAULT '[]'::jsonb,
    revision                    bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at                  timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------- sources & assets
CREATE TABLE dq_source (
    id              uuid PRIMARY KEY,
    side            text NOT NULL UNIQUE CHECK (side IN ('huawei', 'aliyun')),
    role            text NOT NULL CHECK (role IN ('SOURCE', 'TARGET')),
    connection_ref  text NOT NULL,
    capability_ref  text NULL,
    capability      jsonb NOT NULL DEFAULT '{}'::jsonb,
    tls_policy_ref  text NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE dq_import_batch (
    id            uuid PRIMARY KEY,
    kind          text NOT NULL CHECK (kind IN ('ASSET_LIST', 'METRIC_TEMPLATE', 'SAMPLE_TEMPLATE')),
    file_name     text NOT NULL,
    file_digest   bytea NOT NULL CHECK (octet_length(file_digest) = 32),
    mode          text NOT NULL DEFAULT 'CREATE_OR_VALIDATE' CHECK (mode IN ('CREATE_OR_VALIDATE', 'UPDATE_EXPLICIT')),
    options       jsonb NOT NULL DEFAULT '{}'::jsonb,
    status        text NOT NULL DEFAULT 'PREVIEWED' CHECK (status IN ('PREVIEWED', 'CONFIRMED', 'REJECTED')),
    preview       jsonb NOT NULL,
    preview_digest bytea NOT NULL CHECK (octet_length(preview_digest) = 32),
    summary       jsonb NOT NULL DEFAULT '{}'::jsonb,
    reason        text NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    confirmed_at  timestamptz NULL
);

CREATE TABLE dq_asset (
    id               uuid PRIMARY KEY,
    layer            text NOT NULL CHECK (layer IN ('ODS', 'DWD', 'DWS', 'ADS', 'DIM', 'OTHER')),
    source_db        text NOT NULL,
    source_table     text NOT NULL,
    target_db        text NOT NULL,
    target_table     text NOT NULL,
    priority         int NOT NULL DEFAULT 50 CHECK (priority BETWEEN 0 AND 100),
    enabled          boolean NOT NULL DEFAULT true,
    status           text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'ARCHIVED')),
    import_batch_id  uuid NULL REFERENCES dq_import_batch(id),
    revision         bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_db, source_table),
    UNIQUE (target_db, target_table)
);

CREATE TABLE dq_check_rule (
    id                uuid PRIMARY KEY,
    asset_id          uuid NOT NULL REFERENCES dq_asset(id),
    rule_key          text NOT NULL CHECK (char_length(rule_key) BETWEEN 1 AND 64),
    revision          bigint NOT NULL CHECK (revision > 0),
    check_kind        text NOT NULL CHECK (check_kind IN ('COUNT', 'METRIC', 'SAMPLE', 'DEEP')),
    format_version    text NOT NULL DEFAULT 'check-rule-v1.3' CHECK (format_version = 'check-rule-v1.3'),
    definition        jsonb NOT NULL CHECK (jsonb_typeof(definition) = 'object'),
    definition_digest bytea NOT NULL CHECK (octet_length(definition_digest) = 32),
    execution_policy  text NOT NULL DEFAULT 'CENTRAL' CHECK (execution_policy IN ('CENTRAL', 'LOCAL')),
    transfer_class    text NOT NULL CHECK (transfer_class IN ('SUMMARY_ONLY', 'AGGREGATES_APPROVED',
                                                               'SAMPLE_VALUES_APPROVED', 'DETAIL_APPROVED')),
    enabled           boolean NOT NULL DEFAULT true,
    status            text NOT NULL DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'APPROVED', 'SUPERSEDED', 'ARCHIVED')),
    support_state     text NOT NULL DEFAULT 'SUPPORTED' CHECK (support_state IN ('SUPPORTED', 'UNSUPPORTED')),
    support_reason    text NULL,
    approval_ref      text NULL,
    approved_at       timestamptz NULL,
    supersedes_id     uuid NULL REFERENCES dq_check_rule(id),
    import_batch_id   uuid NULL REFERENCES dq_import_batch(id),
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    UNIQUE (asset_id, rule_key, revision),
    UNIQUE (id, asset_id, rule_key, check_kind),
    CHECK (status <> 'APPROVED' OR (approval_ref IS NOT NULL AND approved_at IS NOT NULL)),
    CHECK (supersedes_id IS NULL OR supersedes_id <> id),
    CHECK ((check_kind = 'COUNT' AND transfer_class = 'SUMMARY_ONLY')
        OR (check_kind = 'METRIC' AND transfer_class = 'AGGREGATES_APPROVED')
        OR (check_kind = 'SAMPLE' AND transfer_class = 'SAMPLE_VALUES_APPROVED')
        OR (check_kind = 'DEEP' AND transfer_class = 'DETAIL_APPROVED')),
    CHECK (rule_key <> 'baseline_count' OR check_kind = 'COUNT')
);
CREATE UNIQUE INDEX dq_check_rule_one_approved ON dq_check_rule (asset_id, rule_key) WHERE status = 'APPROVED';

-- ---------------------------------------------------------------- scope
CREATE TABLE dq_manifest (
    id                 uuid PRIMARY KEY,
    version            bigint NOT NULL UNIQUE CHECK (version > 0),
    status             text NOT NULL DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'CONFIRMED', 'SUPERSEDED', 'REJECTED')),
    inventory_complete boolean NOT NULL,
    inventory          jsonb NOT NULL,
    digest             bytea NOT NULL CHECK (octet_length(digest) = 32),
    discovery_run_id   uuid NULL,
    based_on_id        uuid NULL REFERENCES dq_manifest(id),
    summary            jsonb NOT NULL DEFAULT '{}'::jsonb,
    reason             text NULL,
    created_at         timestamptz NOT NULL DEFAULT now(),
    confirmed_at       timestamptz NULL
);
CREATE UNIQUE INDEX dq_manifest_one_confirmed ON dq_manifest ((true)) WHERE status = 'CONFIRMED';

CREATE TABLE dq_scope_entry (
    id                uuid PRIMARY KEY,
    manifest_id       uuid NOT NULL REFERENCES dq_manifest(id),
    asset_id          uuid NOT NULL REFERENCES dq_asset(id),
    scope_kind        text NOT NULL CHECK (scope_kind IN ('ALL', 'RANGE', 'LIST', 'NULL_SCOPE', 'OUTSIDE',
                                                           'INVENTORY_BLOCKER')),
    scope_key         text NOT NULL,
    scope_key_digest  bytea NOT NULL CHECK (octet_length(scope_key_digest) = 32),
    label             text NOT NULL,
    predicate         jsonb NOT NULL,
    presence_source   text NOT NULL CHECK (presence_source IN ('PRESENT', 'ABSENT_CONFIRMED', 'ACCESS_DENIED',
                                                                 'UNKNOWN', 'QUERY_FAILED')),
    presence_target   text NOT NULL CHECK (presence_target IN ('PRESENT', 'ABSENT_CONFIRMED', 'ACCESS_DENIED',
                                                                 'UNKNOWN', 'QUERY_FAILED')),
    expected_reason   text NOT NULL CHECK (expected_reason IN ('IN_SCOPE', 'SOURCE_ONLY', 'TARGET_ONLY', 'UNKNOWN',
                                                                 'DISAPPEARED', 'OUTSIDE', 'TABLE_ABSENT',
                                                                 'ACCESS_DENIED')),
    status            text NOT NULL DEFAULT 'INCLUDED' CHECK (status IN ('INCLUDED', 'EXCLUDED')),
    status_reason     text NULL,
    is_new            boolean NOT NULL DEFAULT false,
    physical          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at        timestamptz NOT NULL DEFAULT now(),
    UNIQUE (manifest_id, asset_id, scope_key)
);
CREATE INDEX dq_scope_entry_asset ON dq_scope_entry (asset_id, scope_key_digest);

CREATE TABLE dq_scope_policy (
    id                uuid PRIMARY KEY,
    asset_id          uuid NOT NULL REFERENCES dq_asset(id),
    scope_key         text NOT NULL,
    scope_key_digest  bytea NOT NULL CHECK (octet_length(scope_key_digest) = 32),
    node_kind         text NOT NULL CHECK (node_kind IN ('TABLE', 'PARTITION')),
    check_selector    jsonb NOT NULL DEFAULT '{"all": true}'::jsonb,
    control_state     text NULL CHECK (control_state IN ('PAUSED', 'CANCELLED', 'ARCHIVED')),
    priority_override int NULL CHECK (priority_override BETWEEN 0 AND 100),
    status            text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'REVOKED')),
    operation_id      uuid NULL,
    reason            text NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT now(),
    revoked_at        timestamptz NULL
);
CREATE INDEX dq_scope_policy_active ON dq_scope_policy (asset_id) WHERE status = 'ACTIVE';

-- ---------------------------------------------------------------- plans & schedules
CREATE TABLE dq_plan (
    id               uuid PRIMARY KEY,
    name             text NOT NULL UNIQUE,
    manifest_mode    text NOT NULL DEFAULT 'LATEST_CONFIRMED' CHECK (manifest_mode IN ('LATEST_CONFIRMED', 'FIXED')),
    manifest_id      uuid NULL REFERENCES dq_manifest(id),
    check_selector   jsonb NOT NULL,
    asset_selector   jsonb NOT NULL DEFAULT '{"all": true}'::jsonb,
    protocol_version text NOT NULL DEFAULT 'srdq-v1.3' CHECK (protocol_version = 'srdq-v1.3'),
    status           text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'ARCHIVED')),
    revision         bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CHECK (manifest_mode <> 'FIXED' OR manifest_id IS NOT NULL)
);

CREATE TABLE dq_schedule (
    id                 uuid PRIMARY KEY,
    plan_id            uuid NOT NULL REFERENCES dq_plan(id),
    name               text NOT NULL UNIQUE,
    kind               text NOT NULL CHECK (kind IN ('CRON', 'INTERVAL', 'CONTINUOUS')),
    cron_expr          text NULL,
    interval_seconds   int NULL CHECK (interval_seconds >= 60),
    rest_seconds       int NULL CHECK (rest_seconds >= 60),
    timezone           text NOT NULL,
    starts_at          timestamptz NOT NULL,
    ends_at            timestamptz NULL,
    overlap            text NOT NULL CHECK (overlap IN ('SKIP', 'COALESCE')),
    misfire            text NOT NULL CHECK (misfire IN ('SKIP', 'LATEST')),
    cancel_on_end      boolean NOT NULL DEFAULT false,
    check_selector     jsonb NOT NULL,
    enabled            boolean NOT NULL DEFAULT true,
    status             text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'ENDED', 'ARCHIVED')),
    revision           bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    next_due_at        timestamptz NULL,
    coalesced          jsonb NULL,
    consecutive_errors int NOT NULL DEFAULT 0,
    backoff_until      timestamptz NULL,
    last_run_id        uuid NULL,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    CHECK (kind <> 'CRON' OR cron_expr IS NOT NULL),
    CHECK (kind <> 'INTERVAL' OR interval_seconds IS NOT NULL),
    CHECK (kind <> 'CONTINUOUS' OR rest_seconds IS NOT NULL),
    CHECK (ends_at IS NULL OR ends_at > starts_at)
);

CREATE TABLE dq_schedule_fire (
    id               uuid PRIMARY KEY,
    schedule_id      uuid NOT NULL REFERENCES dq_schedule(id),
    revision         bigint NOT NULL,
    fire_key         text NOT NULL,
    due_at           timestamptz NOT NULL,
    fired_at         timestamptz NOT NULL DEFAULT now(),
    outcome          text NOT NULL CHECK (outcome IN ('RUN_CREATED', 'SKIPPED_OVERLAP', 'COALESCED', 'MISFIRE_SKIPPED',
                                                     'ERROR')),
    run_id           uuid NULL,
    selector_digest  bytea NOT NULL CHECK (octet_length(selector_digest) = 32),
    detail           jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (schedule_id, revision, fire_key)
);

-- ---------------------------------------------------------------- runs
CREATE TABLE dq_run (
    id                     uuid PRIMARY KEY,
    purpose                text NOT NULL CHECK (purpose IN ('METADATA_PREVIEW', 'COMPARE', 'MANUAL')),
    plan_id                uuid NULL REFERENCES dq_plan(id),
    schedule_id            uuid NULL REFERENCES dq_schedule(id),
    fire_id                uuid NULL REFERENCES dq_schedule_fire(id),
    manifest_id            uuid NULL REFERENCES dq_manifest(id),
    manual_request_id      uuid NULL,
    request_key            text NULL UNIQUE,
    config_snapshot        jsonb NOT NULL,
    control_state          text NOT NULL DEFAULT 'ENABLED' CHECK (control_state IN ('ENABLED', 'PAUSED', 'CANCELLED',
                                                                                   'ARCHIVED')),
    execution_state        text NOT NULL DEFAULT 'READY' CHECK (execution_state IN ('PLANNED', 'READY', 'RUNNING',
                                                    'COMPLETED', 'FAILED', 'CANCEL_PENDING', 'CANCELLED')),
    revision               bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    windows_used           int NOT NULL DEFAULT 0,
    last_window_generation bigint NULL,
    first_window_at        timestamptz NULL,
    last_window_at         timestamptz NULL,
    supersedes_run_id      uuid NULL REFERENCES dq_run(id),
    reason                 text NULL,
    created_at             timestamptz NOT NULL DEFAULT now(),
    completed_at           timestamptz NULL,
    CHECK (purpose = 'METADATA_PREVIEW' OR manifest_id IS NOT NULL)
);
CREATE INDEX dq_run_state ON dq_run (execution_state, control_state);

CREATE TABLE dq_task_node (
    id                uuid PRIMARY KEY,
    run_id            uuid NOT NULL REFERENCES dq_run(id),
    asset_id          uuid NOT NULL REFERENCES dq_asset(id),
    parent_id         uuid NULL,
    kind              text NOT NULL CHECK (kind IN ('TABLE', 'PARTITION', 'SHARD')),
    scope_key         text NOT NULL,
    scope_key_digest  bytea NOT NULL CHECK (octet_length(scope_key_digest) = 32),
    scope_entry_id    uuid NULL REFERENCES dq_scope_entry(id),
    label             text NOT NULL,
    predicate         jsonb NOT NULL,
    control_state     text NOT NULL DEFAULT 'ENABLED' CHECK (control_state IN ('ENABLED', 'PAUSED', 'CANCELLED',
                                                                              'ARCHIVED')),
    priority_override int NULL CHECK (priority_override BETWEEN 0 AND 100),
    refinement_path   text NULL,
    supersedes_node   uuid NULL,
    revision          bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at        timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, asset_id, kind, scope_key),
    UNIQUE (id, run_id, asset_id),
    FOREIGN KEY (parent_id, run_id, asset_id) REFERENCES dq_task_node(id, run_id, asset_id),
    CHECK ((kind = 'TABLE') = (parent_id IS NULL))
);

CREATE TABLE dq_run_check (
    id                     uuid PRIMARY KEY,
    run_id                 uuid NOT NULL,
    asset_id               uuid NOT NULL,
    node_id                uuid NOT NULL,
    rule_id                uuid NOT NULL,
    rule_key               text NOT NULL,
    rule_revision          bigint NOT NULL,
    check_kind             text NOT NULL CHECK (check_kind IN ('COUNT', 'METRIC', 'SAMPLE', 'DEEP')),
    scope_key              text NOT NULL,
    scope_key_digest       bytea NOT NULL CHECK (octet_length(scope_key_digest) = 32),
    -- frozen denominator columns
    expected_evidence      text NOT NULL CHECK (expected_evidence IN ('COUNT', 'METRIC', 'SAMPLE', 'CONTENT_HASH',
                                                                       'EXACT')),
    expected_reason        text NOT NULL,
    rule_snapshot          jsonb NOT NULL,
    execution_snapshot     jsonb NOT NULL,
    rule_digest            bytea NOT NULL CHECK (octet_length(rule_digest) = 32),
    frozen_at              timestamptz NOT NULL,
    -- mutable execution state
    control_state          text NOT NULL DEFAULT 'ENABLED' CHECK (control_state IN ('ENABLED', 'PAUSED', 'CANCELLED',
                                                                                   'ARCHIVED')),
    execution_state        text NOT NULL DEFAULT 'PLANNED' CHECK (execution_state IN ('PLANNED', 'READY', 'RUNNING',
                                                    'COMPLETED', 'BLOCKED', 'FAILED', 'CANCEL_PENDING', 'CANCELLED')),
    priority_override      int NULL CHECK (priority_override BETWEEN 0 AND 100),
    effective_priority     int NOT NULL CHECK (effective_priority BETWEEN 0 AND 100),
    kind_rank              int NOT NULL,
    layer_rank             int NOT NULL,
    ready_seq              bigint NOT NULL,
    revision               bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    observation_seq        bigint NOT NULL DEFAULT 0 CHECK (observation_seq >= 0),
    current_observation_id uuid NULL,
    attempts_failed        int NOT NULL DEFAULT 0,
    next_eligible_at       timestamptz NULL,
    issue                  jsonb NOT NULL DEFAULT '{}'::jsonb,
    -- coverage projection
    coverage_state         text NOT NULL DEFAULT 'PENDING' CHECK (coverage_state IN ('PENDING', 'COMPLETED_EQUAL',
                              'COMPLETED_DIFFERENT', 'INCONCLUSIVE', 'ERROR', 'UNSUPPORTED', 'BLOCKED', 'EXCLUDED')),
    coverage_reason        text NULL,
    current_comparison_id  uuid NULL,
    coverage_updated_at    timestamptz NULL,
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, asset_id, scope_key, rule_key),
    UNIQUE (id, run_id, asset_id),
    FOREIGN KEY (node_id, run_id, asset_id) REFERENCES dq_task_node(id, run_id, asset_id),
    FOREIGN KEY (rule_id, asset_id, rule_key, check_kind) REFERENCES dq_check_rule(id, asset_id, rule_key, check_kind),
    CHECK ((check_kind = 'COUNT' AND expected_evidence = 'COUNT')
        OR (check_kind = 'METRIC' AND expected_evidence = 'METRIC')
        OR (check_kind = 'SAMPLE' AND expected_evidence = 'SAMPLE')
        OR (check_kind = 'DEEP' AND expected_evidence IN ('CONTENT_HASH', 'EXACT')))
);
CREATE INDEX dq_run_check_run_asset ON dq_run_check (run_id, asset_id);
CREATE INDEX dq_run_check_run_kind_state ON dq_run_check (run_id, check_kind, execution_state);
CREATE INDEX dq_run_check_queue ON dq_run_check (control_state, effective_priority DESC, layer_rank, ready_seq);
CREATE INDEX dq_run_check_coverage ON dq_run_check (run_id, check_kind, coverage_state);

CREATE FUNCTION dq_run_check_freeze() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'dq_run_check rows are part of the frozen denominator and cannot be deleted';
    END IF;
    IF NEW.id IS DISTINCT FROM OLD.id OR NEW.run_id IS DISTINCT FROM OLD.run_id
       OR NEW.asset_id IS DISTINCT FROM OLD.asset_id OR NEW.node_id IS DISTINCT FROM OLD.node_id
       OR NEW.rule_id IS DISTINCT FROM OLD.rule_id OR NEW.rule_key IS DISTINCT FROM OLD.rule_key
       OR NEW.rule_revision IS DISTINCT FROM OLD.rule_revision OR NEW.check_kind IS DISTINCT FROM OLD.check_kind
       OR NEW.scope_key IS DISTINCT FROM OLD.scope_key OR NEW.scope_key_digest IS DISTINCT FROM OLD.scope_key_digest
       OR NEW.expected_evidence IS DISTINCT FROM OLD.expected_evidence
       OR NEW.expected_reason IS DISTINCT FROM OLD.expected_reason
       OR NEW.rule_snapshot IS DISTINCT FROM OLD.rule_snapshot
       OR NEW.execution_snapshot IS DISTINCT FROM OLD.execution_snapshot
       OR NEW.rule_digest IS DISTINCT FROM OLD.rule_digest OR NEW.frozen_at IS DISTINCT FROM OLD.frozen_at THEN
        RAISE EXCEPTION 'frozen dq_run_check columns are immutable (run_check %)', OLD.id;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER dq_run_check_freeze BEFORE UPDATE OR DELETE ON dq_run_check
    FOR EACH ROW EXECUTE FUNCTION dq_run_check_freeze();

CREATE TABLE dq_observation (
    id              uuid PRIMARY KEY,
    run_id          uuid NOT NULL,
    asset_id        uuid NOT NULL,
    run_check_id    uuid NOT NULL,
    rule_id         uuid NOT NULL,
    rule_digest     bytea NOT NULL CHECK (octet_length(rule_digest) = 32),
    check_kind      text NOT NULL CHECK (check_kind IN ('COUNT', 'METRIC', 'SAMPLE', 'DEEP')),
    observation_seq bigint NOT NULL CHECK (observation_seq > 0),
    status          text NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'COMPLETED', 'FAILED', 'ABANDONED')),
    abandon_reason  text NULL,
    contract        jsonb NOT NULL,
    context_json    text NOT NULL,
    context_digest  bytea NOT NULL CHECK (octet_length(context_digest) = 32),
    drain           boolean NOT NULL DEFAULT false,
    started_at      timestamptz NULL,
    finished_at     timestamptz NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_check_id, observation_seq),
    UNIQUE (id, run_check_id),
    FOREIGN KEY (run_check_id, run_id, asset_id) REFERENCES dq_run_check(id, run_id, asset_id),
    CHECK (status <> 'ABANDONED' OR abandon_reason IS NOT NULL)
);
CREATE INDEX dq_observation_open ON dq_observation (status) WHERE status = 'OPEN';

CREATE TABLE dq_observation_member (
    id              uuid PRIMARY KEY,
    observation_id  uuid NOT NULL REFERENCES dq_observation(id),
    node_id         uuid NOT NULL REFERENCES dq_task_node(id),
    scan_unit_id    uuid NOT NULL,
    context_json    text NOT NULL,
    context_digest  bytea NOT NULL CHECK (octet_length(context_digest) = 32),
    UNIQUE (observation_id, node_id)
);
CREATE INDEX dq_observation_member_unit ON dq_observation_member (observation_id, scan_unit_id);

-- ---------------------------------------------------------------- executors
CREATE TABLE dq_worker (
    id                 uuid PRIMARY KEY,
    name               text NOT NULL UNIQUE,
    side               text NOT NULL CHECK (side IN ('huawei', 'aliyun')),
    execution_location text NOT NULL DEFAULT 'ALIYUN_ECS',
    pool_ref           text NOT NULL DEFAULT 'central',
    token_digest       bytea NOT NULL UNIQUE CHECK (octet_length(token_digest) = 32),
    status             text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'DISABLED')),
    created_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE dq_worker_registration (
    id                 uuid PRIMARY KEY,
    worker_id          uuid NOT NULL REFERENCES dq_worker(id),
    side               text NOT NULL CHECK (side IN ('huawei', 'aliyun')),
    boot_id            uuid NOT NULL,
    control_generation uuid NOT NULL,
    capability_digest  bytea NOT NULL CHECK (octet_length(capability_digest) = 32),
    capabilities       jsonb NOT NULL,
    status             text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SUPERSEDED', 'EXPIRED')),
    stats              jsonb NOT NULL DEFAULT '{}'::jsonb,
    registered_at      timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX dq_worker_registration_active ON dq_worker_registration (worker_id) WHERE status = 'ACTIVE';

CREATE TABLE dq_work_item (
    id                 uuid PRIMARY KEY,
    run_id             uuid NOT NULL REFERENCES dq_run(id),
    asset_id           uuid NOT NULL REFERENCES dq_asset(id),
    run_check_id       uuid NULL,
    observation_id     uuid NULL REFERENCES dq_observation(id),
    side               text NOT NULL CHECK (side IN ('huawei', 'aliyun')),
    kind               text NOT NULL CHECK (kind IN ('METADATA', 'CHECK', 'DIAGNOSIS')),
    check_kind         text NULL CHECK (check_kind IN ('COUNT', 'METRIC', 'SAMPLE', 'DEEP')),
    scan_unit_id       uuid NOT NULL,
    spec               jsonb NOT NULL,
    result             jsonb NULL,
    status             text NOT NULL DEFAULT 'READY' CHECK (status IN ('READY', 'LEASED', 'DONE', 'FAILED',
                                                   'CANCEL_PENDING', 'CANCELLED', 'ABANDONED')),
    current_attempt_id uuid NULL,
    lease_generation   int NOT NULL DEFAULT 0,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    UNIQUE (observation_id, side, kind, scan_unit_id),
    CHECK (kind = 'METADATA' OR (observation_id IS NOT NULL AND run_check_id IS NOT NULL)),
    FOREIGN KEY (observation_id, run_check_id) REFERENCES dq_observation(id, run_check_id)
);
CREATE INDEX dq_work_item_ready ON dq_work_item (side, status) WHERE status = 'READY';
CREATE INDEX dq_work_item_obs ON dq_work_item (observation_id);

CREATE TABLE dq_attempt (
    id                 uuid PRIMARY KEY,
    work_item_id       uuid NOT NULL REFERENCES dq_work_item(id),
    registration_id    uuid NOT NULL REFERENCES dq_worker_registration(id),
    worker_id          uuid NOT NULL REFERENCES dq_worker(id),
    side               text NOT NULL CHECK (side IN ('huawei', 'aliyun')),
    control_generation uuid NOT NULL,
    lease_generation   int NOT NULL CHECK (lease_generation > 0),
    lease_expires_at   timestamptz NOT NULL,
    status             text NOT NULL DEFAULT 'LEASED' CHECK (status IN ('LEASED', 'RUNNING', 'SUCCEEDED', 'FAILED',
                                             'CANCEL_REQUESTED', 'CANCEL_PENDING', 'CANCELLED', 'REJECTED')),
    cancel_reason      text NULL,
    query_ref          jsonb NULL,
    source_ref         text NOT NULL,
    execution_location text NOT NULL,
    template_digest    bytea NULL,
    window_generation  bigint NOT NULL,
    claimed_at         timestamptz NOT NULL DEFAULT now(),
    read_started       timestamptz NULL,
    read_finished      timestamptz NULL,
    finished_at        timestamptz NULL,
    duration_ms        bigint NULL,
    progress           jsonb NOT NULL DEFAULT '{}'::jsonb,
    error_class        text NULL CHECK (error_class IN ('TIMEOUT', 'CANCELLED', 'ACCESS_DENIED', 'NETWORK',
                                   'QUERY_ERROR', 'STORAGE_SUSPECT', 'BUDGET_EXCEEDED', 'CONTRACT_VIOLATION',
                                   'UNSUPPORTED', 'LOCAL_RESOURCE', 'INTERNAL')),
    error_code         text NULL,
    error_message      text NULL,
    UNIQUE (work_item_id, lease_generation)
);
CREATE INDEX dq_attempt_live ON dq_attempt (side, status)
    WHERE status IN ('LEASED', 'RUNNING', 'CANCEL_REQUESTED', 'CANCEL_PENDING');
CREATE INDEX dq_attempt_finished ON dq_attempt (side, finished_at);

-- ---------------------------------------------------------------- results
CREATE TABLE dq_side_result (
    id               uuid PRIMARY KEY,
    attempt_id       uuid NOT NULL REFERENCES dq_attempt(id),
    work_item_id     uuid NOT NULL REFERENCES dq_work_item(id),
    observation_id   uuid NOT NULL REFERENCES dq_observation(id),
    run_check_id     uuid NOT NULL,
    node_id          uuid NOT NULL REFERENCES dq_task_node(id),
    side             text NOT NULL CHECK (side IN ('huawei', 'aliyun')),
    check_kind       text NOT NULL CHECK (check_kind IN ('COUNT', 'METRIC', 'SAMPLE', 'DEEP')),
    protocol_version text NOT NULL CHECK (protocol_version = 'srdq-v1.3'),
    idempotency_key  text NOT NULL UNIQUE,
    envelope_digest  bytea NOT NULL CHECK (octet_length(envelope_digest) = 32),
    context_digest   bytea NOT NULL CHECK (octet_length(context_digest) = 32),
    evidence         text NOT NULL CHECK (evidence IN ('COUNT', 'METRIC', 'SAMPLE', 'CONTENT_HASH', 'EXACT')),
    payload          jsonb NOT NULL,
    result_complete  boolean NOT NULL,
    total_bytes      bigint NOT NULL DEFAULT 0 CHECK (total_bytes >= 0),
    row_count        bigint NULL CHECK (row_count >= 0),
    validity         text NOT NULL DEFAULT 'STAGED' CHECK (validity IN ('STAGED', 'ACCEPTED', 'REJECTED')),
    reject_reason    text NULL,
    read_started     timestamptz NOT NULL,
    read_finished    timestamptz NOT NULL,
    query_ref        jsonb NULL,
    template_digest  bytea NULL,
    accepted_at      timestamptz NULL,
    raw_purged_at    timestamptz NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (attempt_id, node_id),
    UNIQUE (id, check_kind),
    CHECK (check_kind <> 'COUNT' OR (payload ? 'row_count')),
    CHECK (check_kind <> 'METRIC' OR (payload ? 'group_count' AND payload ? 'metrics_schema_digest')),
    CHECK (check_kind <> 'SAMPLE' OR (payload ? 'sample_rows' AND payload ? 'sample_key_digest'
                                      AND payload ? 'sample_row_digest')),
    CHECK (check_kind <> 'DEEP' OR (payload ? 'row_count' AND payload ? 'digest')),
    CHECK (validity <> 'ACCEPTED' OR (result_complete AND accepted_at IS NOT NULL)),
    CHECK ((check_kind = 'DEEP' AND evidence IN ('CONTENT_HASH', 'EXACT')) OR evidence = check_kind)
);
CREATE INDEX dq_side_result_obs ON dq_side_result (observation_id, validity);

CREATE TABLE dq_metric_group (
    id                  uuid PRIMARY KEY,
    side_result_id      uuid NOT NULL,
    check_kind          text NOT NULL DEFAULT 'METRIC' CHECK (check_kind = 'METRIC'),
    canonical_group_key bytea NOT NULL CHECK (octet_length(canonical_group_key) <= 512),
    group_key_digest    bytea NOT NULL CHECK (octet_length(group_key_digest) = 32),
    group_values        jsonb NOT NULL,
    group_rows          bigint NOT NULL CHECK (group_rows >= 0),
    non_null_guards     jsonb NOT NULL,
    ordinal             bigint NOT NULL CHECK (ordinal >= 0),
    UNIQUE (side_result_id, canonical_group_key),
    UNIQUE (id, side_result_id),
    FOREIGN KEY (side_result_id, check_kind) REFERENCES dq_side_result(id, check_kind)
);
CREATE INDEX dq_metric_group_ordinal ON dq_metric_group (side_result_id, ordinal);
CREATE INDEX dq_metric_group_digest ON dq_metric_group (side_result_id, group_key_digest);

CREATE TABLE dq_metric_value (
    id             uuid PRIMARY KEY,
    side_result_id uuid NOT NULL,
    group_id       uuid NOT NULL,
    metric_id      text NOT NULL CHECK (char_length(metric_id) BETWEEN 1 AND 64),
    value_type     text NOT NULL CHECK (value_type IN ('INTEGER', 'DECIMAL', 'FLOAT32', 'FLOAT64')),
    value_state    text NOT NULL CHECK (value_state IN ('FINITE', 'NULL', 'NAN', 'POS_INF', 'NEG_INF')),
    value_text     text NULL,
    scale          int NULL,
    UNIQUE (group_id, metric_id),
    FOREIGN KEY (group_id, side_result_id) REFERENCES dq_metric_group(id, side_result_id),
    CHECK ((value_state = 'FINITE') = (value_text IS NOT NULL)),
    CHECK ((value_type = 'DECIMAL') = (scale IS NOT NULL AND scale >= 0))
);
CREATE INDEX dq_metric_value_result ON dq_metric_value (side_result_id);

CREATE TABLE dq_comparison (
    id                    uuid PRIMARY KEY,
    run_id                uuid NOT NULL,
    asset_id              uuid NOT NULL,
    run_check_id          uuid NOT NULL,
    observation_id        uuid NOT NULL,
    rule_id               uuid NOT NULL,
    rule_digest           bytea NOT NULL CHECK (octet_length(rule_digest) = 32),
    rule_key              text NOT NULL,
    rule_revision         bigint NOT NULL,
    scope_key             text NOT NULL,
    scope_key_digest      bytea NOT NULL CHECK (octet_length(scope_key_digest) = 32),
    check_kind            text NOT NULL CHECK (check_kind IN ('COUNT', 'METRIC', 'SAMPLE', 'DEEP')),
    evidence              text NOT NULL CHECK (evidence IN ('COUNT', 'METRIC', 'SAMPLE', 'CONTENT_HASH', 'EXACT')),
    verdict               text NOT NULL CHECK (verdict IN ('OBSERVED_EQUAL', 'OBSERVED_DIFFERENT', 'INCONCLUSIVE',
                                                           'ERROR', 'UNSUPPORTED')),
    reason                text NULL,
    revision              bigint NOT NULL CHECK (revision > 0),
    temporal_assurance    text NOT NULL DEFAULT 'NONE' CHECK (temporal_assurance = 'NONE'),
    read_mode             text NOT NULL DEFAULT 'ONLINE_ONLY' CHECK (read_mode = 'ONLINE_ONLY'),
    origin                text NOT NULL DEFAULT 'LIVE' CHECK (origin = 'LIVE'),
    source_result_ids     uuid[] NOT NULL,
    target_result_ids     uuid[] NOT NULL,
    source_read_started   timestamptz NOT NULL,
    source_read_finished  timestamptz NOT NULL,
    target_read_started   timestamptz NOT NULL,
    target_read_finished  timestamptz NOT NULL,
    summary               jsonb NOT NULL,
    difference_count      bigint NOT NULL DEFAULT 0 CHECK (difference_count >= 0),
    difference_count_mode text NOT NULL DEFAULT 'EXACT' CHECK (difference_count_mode IN ('EXACT', 'AT_LEAST', 'UNKNOWN')),
    differences_stored    int NOT NULL DEFAULT 0,
    compared_at           timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_check_id, revision),
    UNIQUE (observation_id),
    FOREIGN KEY (run_check_id, run_id, asset_id) REFERENCES dq_run_check(id, run_id, asset_id),
    FOREIGN KEY (observation_id, run_check_id) REFERENCES dq_observation(id, run_check_id),
    CHECK ((check_kind = 'DEEP' AND evidence IN ('CONTENT_HASH', 'EXACT')) OR evidence = check_kind)
);
CREATE INDEX dq_comparison_trend ON dq_comparison (asset_id, scope_key_digest, rule_key, rule_revision, compared_at);
CREATE INDEX dq_comparison_time ON dq_comparison (compared_at);

ALTER TABLE dq_run_check ADD CONSTRAINT dq_run_check_comparison_fk
    FOREIGN KEY (current_comparison_id) REFERENCES dq_comparison(id);
ALTER TABLE dq_run_check ADD CONSTRAINT dq_run_check_observation_fk
    FOREIGN KEY (current_observation_id) REFERENCES dq_observation(id);

CREATE TABLE dq_difference (
    id            uuid PRIMARY KEY,
    comparison_id uuid NOT NULL REFERENCES dq_comparison(id),
    ordinal       int NOT NULL CHECK (ordinal >= 0),
    type          text NOT NULL CHECK (type IN ('COUNT_DELTA', 'GROUP_SOURCE_ONLY', 'GROUP_TARGET_ONLY',
                        'METRIC_VALUE_DIFF', 'GUARD_DIFF', 'SAMPLE_KEY_SOURCE_ONLY', 'SAMPLE_KEY_TARGET_ONLY',
                        'SAMPLE_FIELD_DIFF', 'DEEP_KEY_SOURCE_ONLY', 'DEEP_KEY_TARGET_ONLY', 'DEEP_FIELD_DIFF',
                        'DEEP_ROW_COUNT_DIFF', 'DEEP_DIGEST_DIFF')),
    group_key     bytea NULL,
    group_values  jsonb NULL,
    metric_id     text NULL,
    key_hex       text NULL,
    key_values    jsonb NULL,
    column_name   text NULL,
    source_value  text NULL,
    target_value  text NULL,
    detail        jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (comparison_id, ordinal),
    CHECK (type <> 'COUNT_DELTA' OR (source_value IS NOT NULL AND target_value IS NOT NULL)),
    CHECK (type NOT IN ('GROUP_SOURCE_ONLY', 'GROUP_TARGET_ONLY', 'GUARD_DIFF') OR group_key IS NOT NULL),
    CHECK (type <> 'METRIC_VALUE_DIFF' OR (group_key IS NOT NULL AND metric_id IS NOT NULL)),
    CHECK (type NOT IN ('SAMPLE_KEY_SOURCE_ONLY', 'SAMPLE_KEY_TARGET_ONLY', 'SAMPLE_FIELD_DIFF',
                        'DEEP_KEY_SOURCE_ONLY', 'DEEP_KEY_TARGET_ONLY', 'DEEP_FIELD_DIFF') OR key_hex IS NOT NULL),
    CHECK (type NOT IN ('SAMPLE_FIELD_DIFF', 'DEEP_FIELD_DIFF') OR column_name IS NOT NULL)
);

CREATE TABLE dq_artifact (
    id                 uuid PRIMARY KEY,
    run_id             uuid NULL REFERENCES dq_run(id),
    run_check_id       uuid NULL,
    rule_id            uuid NULL,
    side_result_id     uuid NULL REFERENCES dq_side_result(id),
    side               text NULL CHECK (side IN ('huawei', 'aliyun')),
    material_scope     text NOT NULL CHECK (material_scope IN ('SAMPLE', 'GROUPS', 'FULL', 'REPORT')),
    rel_path           text NOT NULL,
    projection_digest  bytea NULL,
    content_digest     bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    size_bytes         bigint NOT NULL CHECK (size_bytes >= 0),
    row_count          bigint NULL,
    transfer_class     text NULL,
    purge_state        text NOT NULL DEFAULT 'ACTIVE' CHECK (purge_state IN ('ACTIVE', 'PURGE_DEFERRED', 'PURGED')),
    accepted_at        timestamptz NULL,
    purge_after        timestamptz NULL,
    purged_at          timestamptz NULL,
    detail             jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at         timestamptz NOT NULL DEFAULT now(),
    CHECK (material_scope <> 'REPORT' OR purge_after IS NULL)
);
CREATE INDEX dq_artifact_purge ON dq_artifact (purge_state, purge_after);

-- ---------------------------------------------------------------- diagnosis & operations
CREATE TABLE dq_manual_request (
    id                   uuid PRIMARY KEY,
    request_key          text NOT NULL UNIQUE,
    status               text NOT NULL DEFAULT 'PREVIEWED' CHECK (status IN ('PREVIEWED', 'CONFIRMED', 'REJECTED')),
    check_kind           text NOT NULL CHECK (check_kind IN ('COUNT', 'METRIC', 'SAMPLE', 'DEEP')),
    requested_evidence   text NOT NULL CHECK (requested_evidence IN ('COUNT', 'METRIC', 'SAMPLE', 'CONTENT_HASH',
                                                                         'EXACT')),
    source_mode          text NOT NULL DEFAULT 'LIVE_REREAD' CHECK (source_mode = 'LIVE_REREAD'),
    asset_id             uuid NOT NULL REFERENCES dq_asset(id),
    origin_comparison_id uuid NULL REFERENCES dq_comparison(id),
    request              jsonb NOT NULL,
    request_digest       bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    rule_snapshot        jsonb NULL,
    rule_id              uuid NULL REFERENCES dq_check_rule(id),
    detail_approval_ref  text NULL,
    run_id               uuid NULL REFERENCES dq_run(id),
    preview              jsonb NOT NULL,
    reason               text NOT NULL,
    created_at           timestamptz NOT NULL DEFAULT now(),
    confirmed_at         timestamptz NULL,
    CHECK (check_kind <> 'DEEP' OR status <> 'CONFIRMED' OR detail_approval_ref IS NOT NULL)
);

CREATE TABLE dq_operation (
    id               uuid PRIMARY KEY,
    idempotency_key  text NULL UNIQUE,
    op               text NOT NULL CHECK (op IN ('START', 'RESUME', 'PAUSE', 'CANCEL', 'ARCHIVE', 'RESTORE',
                        'SET_PRIORITY', 'CLEAR_PRIORITY', 'REGENERATE', 'CLOSE_WINDOW', 'EXTEND_WINDOW',
                        'RESUME_CLUSTER', 'CONFIRM_RECOVERY')),
    target_type      text NOT NULL CHECK (target_type IN ('RUN', 'RANGE_NODE', 'RUN_CHECK', 'GLOBAL', 'CLUSTER')),
    target_ids       jsonb NOT NULL,
    effect           text NOT NULL DEFAULT 'CURRENT_ONLY' CHECK (effect IN ('CURRENT_ONLY', 'THIS_AND_FUTURE')),
    check_selector   jsonb NOT NULL DEFAULT '{"all": true}'::jsonb,
    params           jsonb NOT NULL DEFAULT '{}'::jsonb,
    status           text NOT NULL DEFAULT 'PREVIEWED' CHECK (status IN ('PREVIEWED', 'APPLIED', 'REJECTED', 'STALE')),
    preview          jsonb NOT NULL,
    preview_digest   bytea NOT NULL CHECK (octet_length(preview_digest) = 32),
    result           jsonb NULL,
    reason           text NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    applied_at       timestamptz NULL
);

CREATE TABLE dq_outbox (
    id          bigserial PRIMARY KEY,
    event_type  text NOT NULL,
    object_type text NOT NULL,
    object_id   text NULL,
    payload     jsonb NOT NULL DEFAULT '{}'::jsonb,
    reason      text NULL,
    actor       text NOT NULL DEFAULT 'local-user',
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX dq_outbox_time ON dq_outbox (created_at);
CREATE INDEX dq_outbox_object ON dq_outbox (object_type, object_id);

CREATE FUNCTION dq_outbox_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'dq_outbox is append-only';
END $$;
CREATE TRIGGER dq_outbox_append_only BEFORE UPDATE OR DELETE ON dq_outbox
    FOR EACH ROW EXECUTE FUNCTION dq_outbox_append_only();
CREATE TRIGGER dq_outbox_no_truncate BEFORE TRUNCATE ON dq_outbox
    FOR EACH STATEMENT EXECUTE FUNCTION dq_outbox_append_only();

-- ---------------------------------------------------------------- resources & events
CREATE TABLE dq_budget (
    id             uuid PRIMARY KEY,
    run_id         uuid NOT NULL REFERENCES dq_run(id),
    node_id        uuid NULL REFERENCES dq_task_node(id),
    level          text NOT NULL CHECK (level IN ('RUN', 'NODE')),
    resource       text NOT NULL CHECK (resource IN ('TRANSFER_BYTES', 'RESULT_BYTES')),
    limit_amount   numeric NOT NULL CHECK (limit_amount >= 0),
    reserved       numeric NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    consumed       numeric NOT NULL DEFAULT 0 CHECK (consumed >= 0),
    overrun_amount numeric NOT NULL DEFAULT 0 CHECK (overrun_amount >= 0),
    budget_state   text NOT NULL DEFAULT 'OPEN' CHECK (budget_state IN ('OPEN', 'EXCEEDED', 'CLOSED')),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    CHECK ((level = 'RUN') = (node_id IS NULL))
);
CREATE UNIQUE INDEX dq_budget_run_level ON dq_budget (run_id, resource) WHERE level = 'RUN';
CREATE UNIQUE INDEX dq_budget_node_level ON dq_budget (run_id, node_id, resource) WHERE level = 'NODE';

CREATE TABLE dq_asset_slot (
    asset_id    uuid PRIMARY KEY REFERENCES dq_asset(id),
    run_id      uuid NULL REFERENCES dq_run(id),
    state       text NOT NULL DEFAULT 'FREE' CHECK (state IN ('FREE', 'HELD', 'CANCEL_PENDING')),
    acquired_at timestamptz NULL,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    CHECK ((state = 'FREE') = (run_id IS NULL))
);

CREATE TABLE dq_incident (
    id           uuid PRIMARY KEY,
    kind         text NOT NULL CHECK (kind IN ('LOAD_DEGRADE', 'LOAD_RECOVER', 'HARD_STOP', 'CLUSTER_RESUMED',
                       'CANCEL_PENDING_TIMEOUT', 'STORAGE_SUSPECT', 'BUDGET_EXCEEDED', 'WINDOW_CLOSE_CANCEL',
                       'RECOVERY', 'PURGE_DEFERRED', 'LEASE_EXPIRED', 'CAPABILITY_MISSING', 'SR_SIGNAL')),
    dedup_key    text NOT NULL,
    side         text NULL CHECK (side IN ('huawei', 'aliyun')),
    severity     text NOT NULL DEFAULT 'WARN' CHECK (severity IN ('INFO', 'WARN', 'CRITICAL')),
    state        text NOT NULL DEFAULT 'OPEN' CHECK (state IN ('OPEN', 'CLOSED')),
    run_id       uuid NULL,
    run_check_id uuid NULL,
    asset_id     uuid NULL,
    rule_key     text NULL,
    check_kind   text NULL,
    scope_key    text NULL,
    error_code   text NULL,
    message      text NOT NULL,
    occurrences  bigint NOT NULL DEFAULT 1 CHECK (occurrences > 0),
    first_seen   timestamptz NOT NULL DEFAULT now(),
    last_seen    timestamptz NOT NULL DEFAULT now(),
    detail       jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX dq_incident_kind ON dq_incident (kind, last_seen);
CREATE UNIQUE INDEX dq_incident_open_dedup ON dq_incident (kind, dedup_key) WHERE state = 'OPEN';
