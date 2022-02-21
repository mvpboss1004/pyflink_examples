import json
from dateutil.parser import parse
from pyflink.table import EnvironmentSettings, Row, TableEnvironment
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

def sorted_fields(fields):
    return sorted(fields, key=lambda f:f.name)
schema = D.ROW(sorted_fields([
    D.FIELD('name', D.STRING()),
    D.FIELD('birthday', D.TIMESTAMP(3)),
    D.FIELD('location', D.ROW(sorted_fields([
        D.FIELD('lat', D.DOUBLE()),
        D.FIELD('lon', D.DOUBLE())
    ]))),
]))
@udf(result_type=schema)
def json_load(row):
    js = json.loads(row[0])
    ret = Row(
        name = js['name'],
        birthday = parse(js['birthday']),
        location = Row(lat=js['location']['lat'], lon=js['location']['lon'])
    )
    return ret

if __name__ == '__main__':
    b_set = EnvironmentSettings.in_batch_mode() 
    bt_env = TableEnvironment.create(environment_settings=b_set)
    
    # UDF in SQL
    bt_env.create_temporary_system_function('INET_ATON', inet_aton)
    bt_env.create_temporary_system_function('BIT_OR_AGGR', bit_or_aggr)
    bt_env\
        .from_elements(
            [('0.0.0.1',1),('0.0.0.1',2)],
            schema = D.ROW([D.FIELD('ip', D.STRING()), D.FIELD('flag', D.INT())]))\
        .group_by('ip')\
        .select(inet_aton(E.col('ip')), bit_or_aggr(E.col('flag')))\
        .alias('ip', 'flag')\
        .execute()\
        .print()

    # UDF in Table
    bt_env\
        .from_elements(
            [('{"name":"hello", "birthday":"2000-01-01 00:00:00", "location":{"lat":22.0,"lon":113.98} }',)],
            schema = D.ROW([D.FIELD('message', D.STRING())]))\
        .map(json_load)\
        .alias(*schema.field_names())\
        .select(E.col('name'), E.col('birthday'), E.col('location').lat, E.col('location').lon)\
        .execute()\
        .print()

