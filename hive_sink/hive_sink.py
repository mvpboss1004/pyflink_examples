import sys
from datetime import date, datetime
from pyflink.table import EnvironmentSettings, TableEnvironment, SqlDialect
from pyflink.table import types as FT
from pyflink.table.catalog import HiveCatalog

def to_hive_type(data_type):
    if isinstance(data_type, (FT.CharType, FT.VarCharType)):
        return 'STRING'
    elif isinstance(data_type, FT.DecimalType):
        return f'DECIMAL({data_type.precision},{data_type.scale})'
    elif isinstance(data_type, (FT.TimestampType, FT.LocalZonedTimestampType)):
        return 'TIMESTAMP'
    elif isinstance(data_type, FT.ArrayType):
        return f'ARRAY<{to_hive_type(data_type.element_type)}>'
    elif isinstance(data_type, FT.MapType):
        return f'MAP<{to_hive_type(data_type.key_type)}, {to_hive_type(data_type.value_type)}>'
    elif isinstance(data_type, FT.RowType):
        fields = ',\n'.join([f'`{f.name}`: {to_hive_type(f.data_type)}' for f in data_type])
        return f'STRUCT<\n{fields}\n>'
    elif isinstance(data_type, (FT.BooleanType, FT.TinyIntType, FT.SmallIntType, FT.IntType, FT.BigIntType, FT.FloatType, FT.DoubleType, FT.DateType)):
        return data_type.type_name()
    else:
        raise TypeError(f'Unsupported data type {type(data_type)} in hive')
    
def to_hive_schema(schema, partition_fields=[]):
    partition_fields = set(partition_fields)
    fields = [] # ordinary fields
    partitions = [] # partition fields
    for i in range(schema.get_field_count()):
        name = schema.get_field_name(i)
        line = f'`{name}` {to_hive_type(schema.get_field_data_type(i))}'
        if name in partition_fields:
            partitions.append(line)
        else:
            fields.append(line)
    return ',\n'.join(fields), '\n'.join(partitions)

if __name__ == '__main__':
    b_set = EnvironmentSettings.in_batch_mode()
    bt_env = TableEnvironment.create(environment_settings=b_set)
    bt_conf = bt_env.get_config()
    
    bt_env.register_catalog('hive', HiveCatalog('hive', default_database=sys.argv[1], hive_conf_dir=sys.argv[2]))
    bt_env.use_catalog('hive')
    table = bt_env.from_elements(
        elements = [(
            'Alice',
            1,
            ['hello', 'world'],
            {'lat':30.0, 'lon':119.0},
            datetime.now(),
            date.today(),
        )],
        schema = FT.RowType([
            FT.RowField('name', FT.VarCharType(0x7fffffff)),
            FT.RowField('age', FT.IntType()),
            FT.RowField('tags', FT.ArrayType(FT.VarCharType(0x7fffffff))),
            FT.RowField('geo', FT.RowType([
                FT.RowField('lat', FT.DoubleType()),
                FT.RowField('lon', FT.DoubleType()),
            ])),
            FT.RowField('update_time', FT.TimestampType(3)),
            FT.RowField('partition_date', FT.DateType()),
        ])
    )

    fields, partitions = to_hive_schema(table.get_schema(), ['partition_date'])
    bt_conf.set_sql_dialect(SqlDialect.HIVE)
    sql = f'''
        CREATE TABLE IF NOT EXISTS {sys.argv[3]} (
            {fields}
        )
        PARTITIONED BY ({partitions})
        STORED AS ORC
    '''
    print(sql)
    bt_env.execute_sql(sql)
    table.insert_into(sys.argv[3])
    bt_env.execute('hive_sink')

