-- Deleting a member only soft deleted its users row, so the member itself stayed visible
-- in /members, kept its (already revoked) login_code on display and remained eligible for
-- new group chats and key shares.
ALTER TABLE members
    ADD COLUMN deleted_at DATETIME(6);

CREATE INDEX members_tenant_deleted_idx ON members(tenant_id, deleted_at);

UPDATE members m
SET deleted_at = (
    SELECT MAX(u.deleted_at) FROM users u WHERE u.member_id = m.id
)
WHERE EXISTS (
    SELECT 1 FROM users u WHERE u.member_id = m.id AND u.deleted_at IS NOT NULL
)
AND NOT EXISTS (
    SELECT 1 FROM users u WHERE u.member_id = m.id AND u.deleted_at IS NULL
);

UPDATE members SET login_code = NULL WHERE deleted_at IS NOT NULL;
