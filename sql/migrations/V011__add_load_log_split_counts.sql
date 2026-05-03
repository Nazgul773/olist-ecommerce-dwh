USE OlistDWH;
GO

-- Migration V011: Add per-action row counts to load_log.
-- Description: rows_inserted / rows_updated / rows_deleted allow monitoring queries
--              to distinguish incremental (few updates) from first-load (all inserts) runs.
--              NULL = loaded by a pre-V011 SP version (no breakdown available).
-- Applied: manually in SSMS

ALTER TABLE audit.load_log
    ADD rows_inserted INT NULL,
        rows_updated  INT NULL,
        rows_deleted  INT NULL;
GO
