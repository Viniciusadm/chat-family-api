-- Every authenticated media download looks a row up by storage_path, which was an
-- unindexed TEXT column and therefore a full table scan.
ALTER TABLE media_files
    MODIFY storage_path VARCHAR(512) NOT NULL;

CREATE INDEX media_files_storage_path_idx ON media_files(storage_path);
