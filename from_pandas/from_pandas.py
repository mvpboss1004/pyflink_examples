import os
import tempfile
from datetime import datetime

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import types as T
from pyflink.java_gateway import get_gateway
from pyflink.table import EnvironmentSettings, TableEnvironment, Table
from pyflink.table.serializers import ArrowSerializer
from pyflink.table.types import RowField, RowType, create_arrow_schema, from_arrow_type, _to_java_type
from pyflink.table.utils import tz_convert_to_internal
from pyflink.util.java_utils import load_java_class
from pytz import timezone


def safe_type(data_type):
    if isinstance(data_type, pa.Schema):
        for i in range(len(data_type)):
            data_type = data_type.set(i, safe_type(data_type[i]))
        return data_type
    elif isinstance(data_type, pa.Field):
        return data_type.with_type(safe_type(data_type.type))
    elif T.is_decimal(data_type):
        return pa.decimal128(38, 18)
    elif T.is_time(data_type):
        return pa.time32('s')
    elif T.is_timestamp(data_type):
        return pa.timestamp('ms')
    elif T.is_list(data_type):
        return pa.list_(safe_type(data_type.value_type))
    elif T.is_struct(data_type):
        return pa.struct([safe_type(t) for t in data_type])
    elif isinstance(data_type, pa.DataType):
        return data_type
    else:
        raise TypeError(f'Unsupported data type: {type(data_type)}')

def pandas_to_arrow(schema, timezone, field_types, series):
    def create_array(s, t):
        try:
            return pa.Array.from_pandas(s, mask=s.isnull(), type=t, safe=False)
        except pa.ArrowException as e:
            error_msg = 'Exception thrown when converting pandas.Series (%s) to pyarrow.Array (%s).'
            raise RuntimeError(error_msg % (s.dtype, t), e)

    arrays = []
    for i in range(len(schema)):
        s = series[i]
        field_type = field_types[i]
        schema_type = schema.types[i]
        if type(s) == pd.DataFrame:
            array_names = [(create_array(s[s.columns[j]], field.type), field.name)
                           for j, field in enumerate(schema_type)]
            struct_arrays, struct_names = zip(*array_names)
            arrays.append(pa.StructArray.from_arrays(struct_arrays, struct_names))
        else:
            arrays.append(create_array(
                tz_convert_to_internal(s, field_type, timezone), schema_type))
    return pa.RecordBatch.from_arrays(arrays, schema=schema)

class UnsafeSerializer(ArrowSerializer):
    def serialize(self, iterable, stream):
        writer = None
        try:
            for cols in iterable:
                batch = pandas_to_arrow(self._schema, self._timezone, self._field_types, cols)
                if writer is None:
                    writer = pa.RecordBatchStreamWriter(stream, batch.schema)
                writer.write_batch(batch)
        finally:
            if writer is not None:
                writer.close()

def from_pandas(t_env, pdf, splits_num=1):
    if not isinstance(pdf, pd.DataFrame):
        raise TypeError(f'Unsupported type, expected pandas.DataFrame, got {type(pdf)}')
    arrow_schema = safe_type(pa.Schema.from_pandas(pdf, preserve_index=False))
    print(arrow_schema)
    result_type = RowType([RowField(field.name, from_arrow_type(field.type, field.nullable)) for field in arrow_schema])

    # serializes to a file, and we read the file in java
    temp_file = tempfile.NamedTemporaryFile(delete=False, dir=tempfile.mkdtemp())
    serializer = UnsafeSerializer(
        create_arrow_schema(result_type.field_names(), result_type.field_types()),
        result_type,
        timezone(t_env.get_config().get_local_timezone()))
    step = -(-len(pdf) // splits_num)
    pdf_slices = [pdf.iloc[start:start + step] for start in range(0, len(pdf), step)]
    data = [[c for (_, c) in pdf_slice.iteritems()] for pdf_slice in pdf_slices]
    try:
        with temp_file:
            serializer.serialize(data, temp_file)
        jvm = get_gateway().jvm

        data_type = jvm.org.apache.flink.table.types.utils.TypeConversions\
            .fromLegacyInfoToDataType(_to_java_type(result_type))\
            .notNull()\
            .bridgedTo(load_java_class('org.apache.flink.table.data.RowData'))

        j_arrow_table_source = \
            jvm.org.apache.flink.table.runtime.arrow.ArrowUtils.createArrowTableSource(
                data_type, temp_file.name)
        return Table(t_env._j_tenv.fromTableSource(j_arrow_table_source), t_env)
    finally:
        os.unlink(temp_file.name)

if __name__ == '__main__':
    b_set = EnvironmentSettings.in_batch_mode()
    bt_env = TableEnvironment.create(environment_settings=b_set)
    df = pd.DataFrame({
        '_int': [1, 2],
        '_timestamp': [datetime.now(), pd.Timestamp.now().tz_localize('UTC')],
        '_array': [[1,None], [np.nan,2]],
        '_row': [{'_id':'hello'}, {'_id':'world'}],
    })
    print(df)

    # Won't work
    # bt_env.from_pandas(df).execute().print()

    # Works!
    from_pandas(bt_env, df).execute().print()    

