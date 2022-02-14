from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.module import HiveModule

if __name__ == '__main__':
    b_set = EnvironmentSettings.in_batch_mode()
    bt_env = TableEnvironment.create(environment_settings=b_set)
    
    bt_env.load_module('hive', HiveModule('2.3.4'))
    bt_env.use_modules('hive', 'core')
    print('Available functions: ', bt_env.list_functions())
    t0 = bt_env.from_elements([('Alice',1),('Bob',2)], ['name','age'])
    bt_env.register_table('t0', t0)
    bt_env\
        .sql_query('SELECT COLLECT_SET(name) FROM t0')\
        .execute()\
        .print()
