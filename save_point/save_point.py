import sys
import json
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, expressions as E
from pyflink.datastream import StreamExecutionEnvironment

if __name__ == '__main__':
    s_set = EnvironmentSettings\
        .new_instance()\
        .in_streaming_mode()\
        .use_blink_planner()\
        .build()
    st_env = StreamTableEnvironment.create(environment_settings=s_set)
    s_env = StreamExecutionEnvironment.get_execution_environment() 

    st_env.execute_sql(f'''
        CREATE TABLE json_input (
            `id` INT
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{sys.argv[1]}',
            'topic' = '{sys.argv[2]}',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )'''
    )
    st_env\
        .from_path('json_input')\
        .add_columns(E.lit(sys.argv[3]).alias('name'))\
        .execute()\
        .print()
