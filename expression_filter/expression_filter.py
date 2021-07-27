from pyflink.table import EnvironmentSettings, BatchTableEnvironment
from pyflink.table import expressions as E
from pyflink.dataset import ExecutionEnvironment

if __name__ == '__main__':
    b_set = EnvironmentSettings\
        .new_instance()\
        .in_batch_mode()\
        .use_blink_planner()\
        .build()
    bt_env = BatchTableEnvironment.create(environment_settings=b_set)
    b_env = ExecutionEnvironment.get_execution_environment()
    t0 = bt_env.from_elements([('Alice',1),('Bob',2)], ['name','age'])

    condition = "age IN (2,3)"
    # These codes won't work.
    # t0.filter(condition).execute().print()

    # These codes work.
    bt_env.register_table('t0', t0)
    bt_env\
        .sql_query(f'SELECT * FROM t0 WHERE {condition}')\
        .execute()\
        .print()