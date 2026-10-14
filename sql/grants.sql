-- DeliveryPulse privileges. Least privilege, explicit grants (roles: docs/operations.md).
--
-- Run once, on first initialisation of the data volume, after roles.sql and schema.sql,
-- as the bootstrap superuser.
--
--   deliverypulse_owner     owns all schemas and tables (NOLOGIN)
--   deliverypulse_etl       raw: SELECT, INSERT (append-only)
--                           staging: SELECT, INSERT, UPDATE, DELETE, TRUNCATE
--                           analytics: SELECT, INSERT, UPDATE, DELETE
--                           audit: SELECT, INSERT, UPDATE
--                           no CREATE on any schema (no DDL), no MAINTAIN on any table
--                           EXECUTE on staging.analyze_quality_tables() only
--   deliverypulse_readonly  analytics: USAGE + SELECT only; nothing anywhere else
--
-- The target database name comes from the psql variable `dbname` (psql -v dbname=...), because
-- the same grants are applied to the main database and to the `<POSTGRES_DB>_test` database.

-- ---------------------------------------------------------------- database ---
REVOKE ALL ON DATABASE :"dbname" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"dbname" TO deliverypulse_etl, deliverypulse_readonly;
-- Temporary tables for per-batch loading work (the loader).
GRANT TEMPORARY ON DATABASE :"dbname" TO deliverypulse_etl;
ALTER DATABASE :"dbname" SET timezone TO 'UTC';

-- The public schema is not used by DeliveryPulse.
REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- ------------------------------------------------------------ deliverypulse_etl ---
GRANT USAGE ON SCHEMA raw, staging, analytics, audit TO deliverypulse_etl;

-- Includes raw.stream_events: streaming uses the same append-only ETL permissions.
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA raw TO deliverypulse_etl;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA staging TO deliverypulse_etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA analytics TO deliverypulse_etl;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA audit TO deliverypulse_etl;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA raw, staging, audit TO deliverypulse_etl;

-- The quality gate's statistics function (schema.sql). PostgreSQL grants EXECUTE on new
-- functions to PUBLIC by default; that is revoked, so only the ETL role may call it. The ETL
-- role gets no MAINTAIN privilege on any table.
REVOKE ALL ON FUNCTION staging.analyze_quality_tables() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION staging.analyze_quality_tables() TO deliverypulse_etl;

-- ------------------------------------------------------- deliverypulse_readonly ---
GRANT USAGE ON SCHEMA analytics TO deliverypulse_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO deliverypulse_readonly;

-- ----------------------------------------- future objects created by the owner ---
ALTER DEFAULT PRIVILEGES FOR ROLE deliverypulse_owner IN SCHEMA raw
    GRANT SELECT, INSERT ON TABLES TO deliverypulse_etl;
ALTER DEFAULT PRIVILEGES FOR ROLE deliverypulse_owner IN SCHEMA staging
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO deliverypulse_etl;
ALTER DEFAULT PRIVILEGES FOR ROLE deliverypulse_owner IN SCHEMA analytics
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO deliverypulse_etl;
ALTER DEFAULT PRIVILEGES FOR ROLE deliverypulse_owner IN SCHEMA audit
    GRANT SELECT, INSERT, UPDATE ON TABLES TO deliverypulse_etl;
ALTER DEFAULT PRIVILEGES FOR ROLE deliverypulse_owner IN SCHEMA raw, staging, audit
    GRANT USAGE, SELECT ON SEQUENCES TO deliverypulse_etl;

ALTER DEFAULT PRIVILEGES FOR ROLE deliverypulse_owner IN SCHEMA analytics
    GRANT SELECT ON TABLES TO deliverypulse_readonly;

-- Future functions of the owner are not executable by PUBLIC either.
ALTER DEFAULT PRIVILEGES FOR ROLE deliverypulse_owner REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
