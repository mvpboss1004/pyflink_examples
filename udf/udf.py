import json
from pyflink.common.typeinfo import Types as T
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, Row, StreamTableEnvironment, TableEnvironment
from pyflink.table import DataTypes as D, expressions as E
from pyflink.table.udf import udf, udaf
from functools import reduce
from operator import or_

@udf(result_type=D.INT())
def inet_aton(ip):
    try:
        return int(sum([int(n)<<(8*(3-i)) for n,i in zip(ip.split('.'), range(4))]))
    except Exception as e:
        return None

@udaf(result_type=D.INT(), accumulator_type=D.ARRAY(D.INT()), func_type='pandas')
def bit_or_aggr(flags):
    return reduce(or_, flags, 0)

schema = D.ROW([
    D.FIELD('name', D.STRING()),
    D.FIELD('location', D.ROW([
        D.FIELD('lat', D.DOUBLE()),
        D.FIELD('lon', D.DOUBLE())
    ]))
])
type_info = T.ROW_NAMED(
    ['name', 'location'],
    [T.STRING(), T.ROW_NAMED(
        ['lat', 'lon'],
        [T.DOUBLE(), T.DOUBLE()]
    )]
)

def json_load(row):
    js = json.loads(row.message)
    return Row(js['name'], Row(js['location']['lat'], js['location']['lon']))

if __name__ == '__main__':
    s_env = StreamExecutionEnvironment.get_execution_environment()
    st_env = StreamTableEnvironment.create(s_env)
    b_set = EnvironmentSettings.in_batch_mode()
    bt_env = TableEnvironment.create(environment_settings=b_set)
    
    # UDF in SQL
    st_env.create_temporary_system_function('INET_ATON', inet_aton)
    st_env.create_temporary_system_function('BIT_OR_AGGR', bit_or_aggr)
    st_env\
        .from_elements([('0.0.0.1',1),('0.0.0.1',2)], schema=['ip','flag'])\
        .group_by('ip')\
        .select(inet_aton(E.col('ip')), bit_or_aggr(E.col('flag')))\
        .execute()\
        .print()
    
    # UDF in map
    st_env.from_data_stream(
        s_env\
            .from_collection(
                [('{"name":"hello", "location":{"lat":22.0, "lon":113.98"} }',)],
                type_info=T.ROW_NAMED(['message'], [T.STRING()]))\
            .map(json_load, output_type=type_info)\
    ).execute().print()
