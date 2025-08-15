ALTER TABLE dataset.table ADD COLUMN   ingested_at    TIMESTAMP;
ALTER TABLE dataset.table ALTER COLUMN ingested_at    SET DEFAULT CURRENT_TIMESTAMP();
UPDATE      dataset.table SET          ingested_at =  CURRENT_TIMESTAMP WHERE TRUE;