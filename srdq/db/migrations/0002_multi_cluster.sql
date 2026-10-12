SET search_path = srdq;

ALTER TABLE dq_source DROP CONSTRAINT dq_source_side_check;
ALTER TABLE dq_source RENAME COLUMN side TO cluster_id;
ALTER TABLE dq_source ADD COLUMN name text;
ALTER TABLE dq_source ADD COLUMN cloud text;
UPDATE dq_source
SET name = cluster_id,
    cloud = CASE cluster_id WHEN 'huawei' THEN 'HUAWEI' WHEN 'aliyun' THEN 'ALIYUN' ELSE 'OTHER' END;
ALTER TABLE dq_source ALTER COLUMN name SET NOT NULL;
ALTER TABLE dq_source ALTER COLUMN cloud SET NOT NULL;
ALTER TABLE dq_source ADD CONSTRAINT dq_source_cluster_id_format
    CHECK (cluster_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$');
ALTER TABLE dq_source ADD CONSTRAINT dq_source_cloud_nonempty CHECK (cloud <> '');

INSERT INTO dq_source (id, cluster_id, name, cloud, role, connection_ref)
VALUES
    ('00000000-0000-0000-0000-000000000101', 'huawei', 'Huawei', 'HUAWEI', 'SOURCE', 'sr-huawei-ro'),
    ('00000000-0000-0000-0000-000000000102', 'aliyun', 'Aliyun', 'ALIYUN', 'TARGET', 'sr-aliyun-ro')
ON CONFLICT (cluster_id) DO NOTHING;

CREATE TABLE dq_cluster_pair (
    id                text PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$'),
    name              text NOT NULL CHECK (name <> ''),
    source_cluster_id text NOT NULL UNIQUE REFERENCES dq_source(cluster_id),
    target_cluster_id text NOT NULL UNIQUE REFERENCES dq_source(cluster_id),
    enabled           boolean NOT NULL DEFAULT true,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    CHECK (source_cluster_id <> target_cluster_id)
);
INSERT INTO dq_cluster_pair (id, name, source_cluster_id, target_cluster_id)
VALUES ('legacy', 'Legacy', 'huawei', 'aliyun');

ALTER TABLE dq_asset ADD COLUMN pair_id text;
UPDATE dq_asset SET pair_id='legacy';
ALTER TABLE dq_asset ALTER COLUMN pair_id SET NOT NULL;
ALTER TABLE dq_asset ADD CONSTRAINT dq_asset_pair_fk FOREIGN KEY (pair_id) REFERENCES dq_cluster_pair(id);
ALTER TABLE dq_asset DROP CONSTRAINT dq_asset_source_db_source_table_key;
ALTER TABLE dq_asset DROP CONSTRAINT dq_asset_target_db_target_table_key;
ALTER TABLE dq_asset ADD CONSTRAINT dq_asset_pair_source_unique UNIQUE (pair_id, source_db, source_table);
ALTER TABLE dq_asset ADD CONSTRAINT dq_asset_pair_target_unique UNIQUE (pair_id, target_db, target_table);
CREATE INDEX dq_asset_pair ON dq_asset (pair_id, status, enabled);

ALTER TABLE dq_worker DROP CONSTRAINT dq_worker_side_check;
ALTER TABLE dq_worker RENAME COLUMN side TO cluster_id;
ALTER TABLE dq_worker ADD CONSTRAINT dq_worker_cluster_fk FOREIGN KEY (cluster_id) REFERENCES dq_source(cluster_id);

ALTER TABLE dq_worker_registration DROP CONSTRAINT dq_worker_registration_side_check;
ALTER TABLE dq_worker_registration RENAME COLUMN side TO cluster_id;
ALTER TABLE dq_worker_registration ADD CONSTRAINT dq_worker_registration_cluster_fk
    FOREIGN KEY (cluster_id) REFERENCES dq_source(cluster_id);

ALTER TABLE dq_work_item DROP CONSTRAINT dq_work_item_side_check;
ALTER TABLE dq_work_item RENAME COLUMN side TO cluster_id;
ALTER TABLE dq_work_item ADD CONSTRAINT dq_work_item_cluster_fk FOREIGN KEY (cluster_id) REFERENCES dq_source(cluster_id);
DROP INDEX dq_work_item_ready;
CREATE INDEX dq_work_item_ready ON dq_work_item (cluster_id, kind, check_kind, status, created_at)
    WHERE status = 'READY';

ALTER TABLE dq_attempt DROP CONSTRAINT dq_attempt_side_check;
ALTER TABLE dq_attempt RENAME COLUMN side TO cluster_id;
ALTER TABLE dq_attempt ADD CONSTRAINT dq_attempt_cluster_fk FOREIGN KEY (cluster_id) REFERENCES dq_source(cluster_id);
DROP INDEX dq_attempt_live;
CREATE INDEX dq_attempt_live ON dq_attempt (cluster_id, status)
    WHERE status IN ('LEASED', 'RUNNING', 'CANCEL_REQUESTED', 'CANCEL_PENDING');
DROP INDEX dq_attempt_finished;
CREATE INDEX dq_attempt_finished ON dq_attempt (cluster_id, finished_at);

ALTER TABLE dq_side_result DROP CONSTRAINT dq_side_result_side_check;
ALTER TABLE dq_side_result RENAME COLUMN side TO cluster_id;
ALTER TABLE dq_side_result ADD CONSTRAINT dq_side_result_cluster_fk FOREIGN KEY (cluster_id) REFERENCES dq_source(cluster_id);
ALTER TABLE dq_side_result DROP CONSTRAINT dq_side_result_protocol_version_check;
ALTER TABLE dq_side_result ADD CONSTRAINT dq_side_result_protocol_version_check
    CHECK (protocol_version IN ('srdq-v1.3', 'srdq-v1.4'));

ALTER TABLE dq_artifact DROP CONSTRAINT dq_artifact_side_check;
ALTER TABLE dq_artifact RENAME COLUMN side TO cluster_id;
ALTER TABLE dq_artifact ADD CONSTRAINT dq_artifact_cluster_fk FOREIGN KEY (cluster_id) REFERENCES dq_source(cluster_id);

ALTER TABLE dq_incident DROP CONSTRAINT dq_incident_side_check;
ALTER TABLE dq_incident RENAME COLUMN side TO cluster_id;
ALTER TABLE dq_incident ADD CONSTRAINT dq_incident_cluster_fk FOREIGN KEY (cluster_id) REFERENCES dq_source(cluster_id);

ALTER TABLE dq_plan DROP CONSTRAINT dq_plan_protocol_version_check;
ALTER TABLE dq_plan ALTER COLUMN protocol_version SET DEFAULT 'srdq-v1.4';
ALTER TABLE dq_plan ADD CONSTRAINT dq_plan_protocol_version_check
    CHECK (protocol_version IN ('srdq-v1.3', 'srdq-v1.4'));
