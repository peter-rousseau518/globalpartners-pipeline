/* =====================================================================
   GlobalPartners - read-only login for the AWS Glue ingest job
   03_create_glue_reader.sql
   Replace the password below before running. Do NOT commit the real
   password to GitHub - keep this placeholder in the repo copy.
   ===================================================================== */
USE master;
GO
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'glue_reader')
    CREATE LOGIN glue_reader WITH PASSWORD = 'CHANGE_ME_Strong#Passw0rd', CHECK_POLICY = ON;
GO
USE GlobalPartners;
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'glue_reader')
    CREATE USER glue_reader FOR LOGIN glue_reader;
ALTER ROLE db_datareader ADD MEMBER glue_reader;   -- SELECT only, no writes
GO
