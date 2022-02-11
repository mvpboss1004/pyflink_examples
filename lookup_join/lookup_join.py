import sys
from pyflink.table import EnvironmentSettings, TableEnvironment

if __name__ == '__main__':
    s_set = EnvironmentSettings.in_streaming_mode()
    st_env = TableEnvironment.create(environment_settings=s_set)
    st_env.execute_sql(f'''
        CREATE TABLE `left` (
            `eman` STRING
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{sys.argv[1]}',
            'topic' = '{sys.argv[2]}',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset',
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
