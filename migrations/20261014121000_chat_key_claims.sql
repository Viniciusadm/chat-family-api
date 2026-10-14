CREATE TABLE chat_key_claims (
    chat_id BINARY(16) PRIMARY KEY,
    tenant_id BINARY(16) NOT NULL,
    device_id BINARY(16) NOT NULL,
    claimed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT chat_key_claims_chat_fk FOREIGN KEY (chat_id) REFERENCES chats(id) ON DELETE CASCADE,
    CONSTRAINT chat_key_claims_device_fk FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
