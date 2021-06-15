import sys
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, Table
from pyflink.table import expressions as E
from pyflink.datastream import DataStream, StreamExecutionEnvironment

if __name__ == '__main__':
    s_set = EnvironmentSettings\
        .new_instance()\
        .in_streaming_mode()\
        .use_blink_planner()\
        .build()
    st_env = StreamTableEnvironment.create(environment_settings=s_set)
    s_env = StreamExecutionEnvironment.get_execution_environment() 
    st_env.execute_sql(f'''
        CREATE TABLE `left` (
            `eman` STRING
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{sys.argv[1]}',
            'topic' = '{sys.argv[2]}',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true',
            'json.timestamp-format.standard' = 'SQL'
        )'''
    )
    left = st_env.from_path('`left`')
    st_env.execute_sql(f'''
        CREATE TABLE `right` (
            name STRING NULL,
            age INT NULL
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{sys.argv[3]}',
            'table-name' = '{sys.argv[4]}',
            'username' = '{sys.argv[5]}',
            'password' = '{sys.argv[6]}',
            'lookup.cache.ttl' = '60s',
            'lookup.cache.max-rows' = '100'
        )'''
    )
    right = st_env.from_path('`right`')
    left\
        .left_outer_join(right, left.eman==right.name)\
        .execute()\
        .print()
