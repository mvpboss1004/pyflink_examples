import sys
from datetime import date
from pyflink.table import EnvironmentSettings, BatchTableEnvironment
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
    bt_env.register_catalog('hive', HiveCatalog('hive', default_database=sys.argv[1], hive_conf_dir=sys.argv[2]))
    bt_env.use_catalog('hive')
    sql = f'''
        CREATE TABLE IF NOT EXISTS {sys.argv[3]} (
            _id INT,
            message STRING
        )
        PARTITIONED BY (partition_date DATE)
        STORED AS orc
    '''
    today = str(date.today())
    bt_env\
        .from_elements([(1,'hello\nworld',today),(2,'are\nyou\nok',today)], ['_id','message','partition_date'])\
        .execute_insert(sys.argv[3], overwrite=True)

