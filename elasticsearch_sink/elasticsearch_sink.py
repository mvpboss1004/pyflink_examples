import sys
from datetime import datetime
from pyflink.table import DataTypes as DT, EnvironmentSettings, TableEnvironment
if __name__ == '__main__':
    b_set = EnvironmentSettings.in_batch_mode()
    bt_env = TableEnvironment.create(environment_settings=b_set)
    sql = f'''
        CREATE TABLE test (
            name STRING,
            age INT,
            tags ARRAY<STRING>,
            birthday TIMESTAMP(3),
            location ROW<
                lat DOUBLE,
                lon DOUBLE
            >
        ) WITH (
            'connector' = 'elasticsearch-7',
            'hosts' = '{sys.argv[1]}',
            'index' = 'test-{{birthday|yyyy.MM.dd}}',
            'username' = '{sys.argv[2]}',
            'password' = '{sys.argv[3]}',
            'failure-handler' = 'retry-rejected',
            'sink.bulk-flush.backoff.max-retries' = '3'
        )
    '''
    bt_env.execute_sql(sql)
    schema = DT.ROW([
        DT.FIELD('name', DT.STRING()),
        DT.FIELD('age', DT.INT()),
        DT.FIELD('tags', DT.ARRAY(DT.STRING())),
        DT.FIELD('birthday', DT.TIMESTAMP(3)),
        DT.FIELD('location', DT.ROW([
            DT.FIELD('lat', DT.DOUBLE()),
            DT.FIELD('lon', DT.DOUBLE())
        ]))
    ])
    element = ('Alice', 1, ('girl','baby'), datetime(2000,1,1), {'lat':22.0, 'lon':113.0})
    bt_env.from_elements([element], schema=schema).insert_into('test')
    bt_env.execute('elasticsearch_sink')
