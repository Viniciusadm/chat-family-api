-- Editing or soft deleting a message never moves created_at, so a client that syncs with
-- a created_at cursor can never see those changes again. updated_at is bumped by every
-- write and gives the client a cursor that also covers messages it already stored.
ALTER TABLE messages
    ADD COLUMN updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6);

UPDATE messages
SET updated_at = GREATEST(
    created_at,
    COALESCE(edited_at, created_at),
    COALESCE(deleted_at, created_at)
);

CREATE INDEX messages_chat_updated_idx ON messages(chat_id, updated_at);
