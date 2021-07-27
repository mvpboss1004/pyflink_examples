from pyflink.table import EnvironmentSettings, BatchTableEnvironment
from pyflink.table import expressions as E
from pyflink.table.module import HiveModule
from pyflink.dataset import ExecutionEnvironment

if __name__ == '__main__':
    b_set = EnvironmentSettings\
        .new_instance()\
        .in_batch_mode()\
        .use_blink_planner()\
        .build()
    bt_env = BatchTableEnvironment.create(environment_settings=b_set)
    b_env = ExecutionEnvironment.get_execution_environment()
    bt_env.load_module('hive', HiveModule())
    print('Available functions: ', bt_env.list_functions())
    t0 = bt_env.from_elements([('Alice',1),('Bob',2)], ['name','age'])
    bt_env.register_table('t0', t0)
    bt_env\
        .sql_query('SELECT COLLECT_SET(name) FROM t0')\
        .execute()\
        .print()