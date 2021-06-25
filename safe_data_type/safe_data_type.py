import pyarrow as pa
import pandas as pd
from datetime import datetime, time, timedelta
from pyflink.table import EnvironmentSettings, BatchTableEnvironment, types as T

def safe_data_type(data_type):
   if isinstance(data_type, T.DecimalType):
       data_type.precision = 38
       data_type.scale = 18
   elif isinstance(data_type, T.TimeType):
       data_type.precision = 0
   elif isinstance(data_type, (T.TimestampType, T.LocalZonedTimestampType)):
       data_type.precision = 3
   elif isinstance(data_type, T.VarCharType):
       data_type.length = 0x7ffffff
   elif isinstance(data_type, T.VarBinaryType):
       data_type.length = 0x7fffffff
   elif isinstance(data_type, T.YearMonthIntervalType):
       data_type.resolution = T.YearMonthIntervalType.YearMonthResolution.MONTH
       data_type.precision = 2
   elif isinstance(data_type, T.DayTimeIntervalType):
       data_type.resolution = T.DayTimeIntervalType.DayTimeResolution.SECOND
       data_type.day_precision = 2
       data_type.fractional_precision = 3
   elif isinstance(data_type, (T.ArrayType, T.MultisetType)):
       data_type.element_type = safe_data_type(data_type.element_type)
   elif isinstance(data_type, T.ListViewType):
       data_type._element_type = safe_data_type(data_type._element_type)
   elif isinstance(data_type, T.MapType):
       data_type.key_type = safe_data_type(data_type.key_type)
       data_type.value_type = safe_data_type(data_type.value_type)
   elif isinstance(data_type, T.MapViewType):
       data_type._key_type = safe_data_type(data_type._key_type)
       data_type._value_type = safe_data_type(data_type._value_type)
   elif isinstance(data_type, T.RowField):
       data_type.data_type = safe_data_type(data_type.data_type)
   elif isinstance(data_type, T.RowType):
       for i in range(data_type.fields):
           data_type.fields[i] = safe_data_type(data_type.fields[i])
   elif isinstance(data_type, T.DataType):
       pass
   else:
       raise TypeError(f'Unsupported data type: {type(data_type)}')
   return data_type

def from_pandas(t_env:TableEnvironment, pdf:pd.DataFrame) -> Table:
   schema = T.RowType([
       T.RowField(field.name, safe_data_type(T.from_arrow_type(field.type, field.nullable)))\
       for field in pa.Schema.from_pandas(pdf, preserve_index=False)
   ])
   return t_env.from_pandas(pdf, schema)

if __name__ == '__main__':
    b_set = EnvironmentSettings\
        .new_instance()\
        .in_batch_mode()\
        .use_blink_planner()\
        .build()
    bt_env = BatchTableEnvironment.create(environment_settings=b_set)
    df = pd.DataFrame({
        '_int': [1L],
        '_time': [Time(12,34,56)],
        '_timestamp': [datetime.now()],
        '_date_interval': [timedelta(days=1)],
        '_time_interval': [timedelta(seconds=1)],
        '_array_int': [(1,)],
        '_row': [{'_id': 'helloworld'}]
    })

    # Won't work
    # bt_env.from_pandas(df).execute().print()

    # Works!
    from_pandas(bt_env, df).execute().print()    

    
