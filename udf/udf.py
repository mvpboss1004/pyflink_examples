from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table import expressions as E, DataTypes as T
from pyflink.table.udf import udf, udaf
from functools import reduce
from operator import or_

@udf(result_type=T.INT())
def inet_aton(ip):
    try:
        return int(sum([int(n)<<(8*(3-i)) for n,i in zip(ip.split('.'), range(4))]))
    except Exception as e:
        return None

@udaf(result_type=T.INT(), accumulator_type=T.ARRAY(T.INT()), func_type='pandas')
def bit_or_aggr(flags):
    return reduce(or_, flags, 0)

if __name__ == '__main__':
    b_set = EnvironmentSettings.in_batch_mode()
    bt_env = TableEnvironment.create(environment_settings=b_set)
    bt_env.create_temporary_system_function('INET_ATON', inet_aton)
    bt_env.create_temporary_system_function('BIT_OR_AGGR', bit_or_aggr)
    bt_env\
        .from_elements([('0.0.0.1',1),('0.0.0.1',2)], schema=['ip','flag'])\
        .group_by('ip')\
        .select(inet_aton(E.col('ip')), bit_or_aggr(E.col('flag')))\
        .execute()\
        .print()

