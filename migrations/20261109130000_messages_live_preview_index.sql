-- The preview lookup must skip deleted history without scanning it or sorting
-- the remaining messages. The UUID breaks timestamp ties in the same order as
-- refresh_chat_preview.
CREATE INDEX messages_chat_live_created_idx
    ON messages(chat_id, is_deleted, created_at DESC, id DESC);
