# PyFlink Examples

PyFlink was added to flink since 1.9. Unfortunately, it's still lack of documentation.  
This project is a collection of examples demonstrate the usage of pyflink. Since many features were added in 1.12, I strongly suggest you to use pyflink 1.12+. Codes here are tested on 1.13.   
Examples:
 - [external_catalog](https://github.com/mvpboss1004/pyflink_examples/tree/master/external_catalog): Use postgresql and hive as external catalog
 - [from_pandas](https://github.com/mvpboss1004/pyflink_examples/tree/master/from_pandas): Safely convert pandas dataframe to flink table
 - [hive_module](https://github.com/mvpboss1004/pyflink_examples/tree/master/hive_module): Load hive module then use hive built-in functions
 - [hive_sink](https://github.com/mvpboss1004/pyflink_examples/tree/master/hive_sink): Save flink table into hive without manually creating hive table
 - [json_input](https://github.com/mvpboss1004/pyflink_examples/tree/master/json_input): See this example if you have trouble in defining json-format source
 - [lookup_join](https://github.com/mvpboss1004/pyflink_examples/tree/master/lookup_join): Stream table(left) join jdbc table(right)
 - [socket_stream](https://github.com/mvpboss1004/pyflink_examples/tree/master/socket_stream): How to create `socketTextStream` in pyflink
 - [udf](https://github.com/mvpboss1004/pyflink_examples/tree/master/udf): Create udf and udaf
 