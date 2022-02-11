from pyflink.table import EnvironmentSettings, TableEnvironment

if __name__ == '__main__':
    b_set = EnvironmentSettings.in_batch_mode()
    bt_env = TableEnvironment.create(environment_settings=b_set)
    t0 = bt_env.from_elements([('Alice',1),('Bob',2)], ['name','age'])

    condition = 'age IN (2,3)'
    # These codes won't work.
    # t0.where(condition).execute().print()

    # These codes work.
    bt_env.register_table('t0', t0)
    bt_env\
        .sql_query(f'SELECT * FROM t0 WHERE {condition}')\
        .execute()\
        .print()
