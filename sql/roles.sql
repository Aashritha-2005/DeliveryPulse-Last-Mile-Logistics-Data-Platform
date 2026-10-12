-- DeliveryPulse roles (least privilege; see docs/operations.md, "Roles").
--
-- Run once, on first initialisation of the data volume, by
-- docker/postgres/init-deliverypulse.sh as the bootstrap superuser.
-- Order: roles.sql -> schema.sql -> grants.sql.
--
-- Passwords are read from the container environment with psql's \getenv, so they never
-- appear in this file or on a command line.

\getenv etl_password DELIVERYPULSE_ETL_PASSWORD
\getenv readonly_password DELIVERYPULSE_READONLY_PASSWORD

-- Owns every DeliveryPulse schema and table. It cannot log in, so no application
-- connection can ever run DDL as the owner.
CREATE ROLE deliverypulse_owner
    NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;

-- Pipeline writer: DML on raw / staging / analytics / audit, no DDL (see grants.sql).
CREATE ROLE deliverypulse_etl
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
    PASSWORD :'etl_password';

-- Dashboards and analysts: SELECT on analytics tables only (see grants.sql).
CREATE ROLE deliverypulse_readonly
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
    PASSWORD :'readonly_password';
