CREATE SCHEMA IF NOT EXISTS vectorlake;

CREATE TABLE IF NOT EXISTS vectorlake.tenants (
    tenant_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vectorlake.dedup_scopes (
    scope_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES vectorlake.tenants(tenant_id),
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS vectorlake.model_versions (
    model_version_id TEXT PRIMARY KEY,
    model_name TEXT NOT NULL,
    preprocess_version TEXT NOT NULL,
    embedding_dimension INTEGER NOT NULL CHECK (embedding_dimension > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vectorlake.dedup_policies (
    policy_version TEXT PRIMARY KEY,
    config JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO vectorlake.tenants (tenant_id, name)
VALUES ('local-dev', 'Local development')
ON CONFLICT (tenant_id) DO NOTHING;

INSERT INTO vectorlake.dedup_scopes (scope_id, tenant_id, name)
VALUES ('local-dev', 'local-dev', 'Default')
ON CONFLICT (scope_id) DO NOTHING;
