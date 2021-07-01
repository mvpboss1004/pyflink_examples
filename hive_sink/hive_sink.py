import sys
from datetime import datetime, time, tzinfo
from pyflink.table import EnvironmentSettings, BatchTableEnvironment, SqlDialect
from pyflink.table import expressions as E, types as T
from pyflink.table.catalog import HiveCatalog
from pyflink.dataset import ExecutionEnvironment

if __name__ == '__main__':
    b_set = EnvironmentSettings\
        .new_instance()\
        .in_batch_mode()\
        .use_blink_planner()\
        .build()
    bt_env = BatchTableEnvironment.create(environment_settings=b_set)
    b_env = ExecutionEnvironment.get_execution_environment()
    bt_conf = bt_env.get_config()
    
    bt_env.register_catalog('hive', HiveCatalog('hive', default_database=sys.argv[1], hive_conf_dir=sys.argv[2]))
    bt_env.use_catalog('hive')
    bt_conf.set_sql_dialect(SqlDialect.HIVE)
    sql = f'''
        CREATE TABLE IF NOT EXISTS {sys.argv[3]} (
            multiset_ ARRAY<INT>,
            time_ TIMESTAMP,
            timestamp_tz_ TIMESTAMP WITH TIME ZONE,
            timestamp_ltz_ TIMESTAMP WITH LOCAL TIME ZONE,
            struct_ STRUCT<
                id_: STRING
            >
        )
        STORED AS ORC
    '''
    bt_env.execute_sql(sql)
    bt_env\
        .from_elements(
            elements = [
                set(1,2),
                time(12,0,0),
                datetime(2000,1,1, tzinfo=tzinfo('Asia/Shanghai')),
                datetime().now(),
            ],
            schema = T.RowType([
                T.RowField('multiset_', T.MultiSetType(T.IntType())),
                T.RowField('time_', T.TimeType(0)),
                T.RowField('timestamp_tz_', T.ZonedTimestampType(3)),
                T.RowField('timestamp_ltz_', T.LocalZonedTimestampType(3)),
                T.RowField('struct_', T.RowType([T.RowField('id_',T.StringType())])),
            ])
        )\
        .execute_insert(sys.argv[3], overwrite=True)

