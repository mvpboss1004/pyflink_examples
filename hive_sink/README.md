# Hive Sink
These codes automatically create table in hive for your flink table and insert into it.

How to use:
 - Setup a hive server.
 - Run `flink run -t local -py hive_sink.py your_hive_database your_hive_conf_dir your_hive_table` to start this application.
 - Then you'll see one record inserted into your table.

