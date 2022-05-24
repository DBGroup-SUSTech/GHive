SELECT 'Upgrading MetaStore schema from 0.12.0 to 0.13.0' AS Status from dual;

@016-HIVE-6386.oracle.sql;
@017-HIVE-6458.oracle.sql;
@018-HIVE-6757.oracle.sql;
@hive-txn-schema-0.13.0.oracle.sql;

UPDATE VERSION SET SCHEMA_VERSION='0.13.0', VERSION_COMMENT='Hive release version 0.13.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 0.12.0 to 0.13.0' AS Status from dual;
