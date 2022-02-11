import sys
from pyflink.table import EnvironmentSettings, TableEnvironment, expressions as E

if __name__ == '__main__':
    s_set = EnvironmentSettings.in_streaming_mode()
    st_env = StreamTableEnvironment.create(environment_settings=s_set)

    st_env.execute_sql(f'''
        CREATE TEMPORARY TABLE kafka_in (
            `id` INT
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{sys.argv[1]}',
            'properties.group.id' = 'save_point',
            'topic' = '{sys.argv[2]}',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )'''
    )
    st_env.execute_sql(f'''
        CREATE TEMPORARY TABLE kafka_out (
            `id` INT,
            `name` STRING
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{sys.argv[1]}',
            'topic' = '{sys.argv[3]}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )'''
    )
    st_env\
        .from_path('kafka_in')\
        .add_columns(E.lit(sys.argv[4]).alias('name'))\
        .insert_into('kafka_out')
    st_env.execute('save_point')

