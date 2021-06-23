import sys
from datetime import date
from pyflink.table import EnvironmentSettings, BatchTableEnvironment, SqlDialect
from pyflink.table import expressions as E, DataTypes as T
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
            _id INT,
            message STRING
        )
        PARTITIONED BY (partition_date DATE)
        STORED AS ORC
    '''
    bt_env.execute_sql(sql)
    bt_env\
        .from_elements(
            elements = [
                (1, 'hello\nworld', date.today()),
                (2, 'are\nyou\nok', date.today())
            ],
            schema = T.ROW([
                T.FIELD('_id', T.INT()),
                T.FIELD('message', T.STRING()),
                T.FIELD('partition_date', T.DATE())
            ])
        )\
        .execute_insert(sys.argv[3], overwrite=True)

