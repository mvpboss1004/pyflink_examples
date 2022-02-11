import sys
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import HiveCatalog, JdbcCatalog

if __name__ == '__main__':
    b_set = EnvironmentSettings.in_batch_mode()
    bt_env = TableEnvironment.create(environment_settings=b_set)

    for name, catalog in [
        ('jdbc', JdbcCatalog('jdbc', base_url=sys.argv[1], default_database=sys.argv[2], username=sys.argv[3], pwd=sys.argv[4])),
        ('hive', HiveCatalog('hive', default_database=sys.argv[5], hive_conf_dir=sys.argv[6]))
    ]:
        bt_env.register_catalog(name, catalog)
        bt_env.use_catalog(name)
        print(f'{name} catalog tables:', bt_env.list_tables())
