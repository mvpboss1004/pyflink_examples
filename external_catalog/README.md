# External Catalog
PyFlink supports two types of external catalog: jdbc (current only postgresql) and hive. This demo connects both types and show tables.

How to use:
 - Setup postgresql database and table for testing.
 - Setup hive database and table for tesing.
 - Run `flink run -t local -py external_catalog.py jdbc:postgresql://host:port pg_db pg_user pg_passwd hive_db hive_conf_dir` to start this application.