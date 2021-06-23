from pyflink.table import EnvironmentSettings, BatchTableEnvironment
from pyflink.table import expressions as E, DataTypes as T
from pyflinm.table.udf import udf, udaf
from pyflink.dataset import ExecutionEnvironment
from functools import reduce
from operator import or_

@udf(result_type=T.INT())
def inet_aton(ip:str) -> int:
    try:
        sum([int(n)<<(8*(3-i)) for n,i in zip(ip.split('.'),range(4))])
    except Exception as e:
        return None

@udaf(result_type=T.INT())
def bit_or_aggr(flags: int) -> int:
    return reduce(flags, or_, 0)

if __name__ == '__main__':
    b_set = EnvironmentSettings\
        .new_instance()\
        .in_batch_mode()\
        .use_blink_planner()\
        .build()
    bt_env = BatchTableEnvironment.create(environment_settings=b_set)
    b_env = ExecutionEnvironment.get_execution_environment()
    bt_env.register_function('INET_ATON', inet_aton)
    bt_env.register_function('BIT_OR_AGGR', inet_aton)
    bt_env\
        .from_elements([('0.0.0.1',1),('0.0.0.1',2)], ['ip','flag'])\
        .group_by(E.call_sql('INET_ATON(ip) AS ip_int'))\
        .aggregate(bit_or_aggr(E.col('flag')).alias('flag_agg'))\
        .execute()\
        .print()
    