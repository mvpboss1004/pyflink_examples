import sys
from pyflink.table import EnvironmentSettings, BatchTableEnvironment
from pyflink.table import expressions as E
from pyflink.table.catalog import HiveCatalog, JdbcCatalog
from pyflink.dataset import ExecutionEnvironment

if __name__ == '__main__':
    b_set = EnvironmentSettings\
        .new_instance()\
        .in_batch_mode()\
        .use_blink_planner()\
        .build()
    bt_env = BatchTableEnvironment.create(environment_settings=b_set)
    b_env = ExecutionEnvironment.get_execution_environment()

    for name, catalog in [
        ('jdbc', JdbcCatalog(name, base_url=sys.argv[1], default_database=sys.argv[2], username=sys.argv[3], pwd=sys.argv[4])),
        ('hive', HiveCatalog(name, default_database=sys.argv[5], hive_conf_dir=sys.argv[6]))
    ]:
        bt_env.register_catalog(name, catalog)
        bt_env.use_catalog(name)
        print(f'{name} catalog tables:', bt_env.list_tables())
