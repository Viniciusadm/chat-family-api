CREATE TABLE child_mutes (
    tenant_id BINARY(16) NOT NULL,
    child_member_id BINARY(16) NOT NULL,
    muted_by_member_id BINARY(16) NOT NULL,
    scope ENUM('direct', 'all') NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (child_member_id, muted_by_member_id, scope),
    CONSTRAINT child_mutes_tenant_fk FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
    CONSTRAINT child_mutes_child_fk FOREIGN KEY (child_member_id) REFERENCES members(id) ON DELETE CASCADE,
    CONSTRAINT child_mutes_muted_by_fk FOREIGN KEY (muted_by_member_id) REFERENCES members(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX child_mutes_tenant_expires_idx ON child_mutes(tenant_id, expires_at);
