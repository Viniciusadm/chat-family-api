-- Soft deleting a text message clears its ciphertext, which the original constraint
-- rejected, so DELETE /chats/{id}/messages/{id} always failed once the query syntax
-- was fixed. Deleted rows carry no payload and are exempt.
ALTER TABLE messages
    DROP CONSTRAINT messages_text_is_encrypted;

ALTER TABLE messages
    ADD CONSTRAINT messages_text_is_encrypted CHECK (
        type <> 'text'
        OR is_deleted
        OR (ciphertext IS NOT NULL AND iv IS NOT NULL AND enc_version IS NOT NULL)
    );
