import numpy as np
import pyarrow as pa
import pandas as pd
from pyflink.table import EnvironmentSettings, BatchTableEnvironment

def from_pandas_type(data_type):
    if isinstance(data_type, np.nan):
        return None 
    elif isinstance(data_type, pd.Timestamp):
        return data_type.replace(nanosecond=0).to_pydatetime()
    elif isinstance(data_type, list):
        retrun list(from_pandas_type(d) for d in data_type)
    elif isinstance(data_type, tuple):
        retrun tuple(from_pandas_type(d) for d in data_type)
    elif isinstance(data_type, dict):
        retrun {from_pandas_type(k):from_pandas_type(data_type[k]) for k in data_type}
    else:
        return data_type

def from_pandas(t_env, pdf):
    df = from_pandas_type(pdf.to_dict(orient='dict'))
    return t_env.from_elements(list(zip(*df.values())), list(df.keys()))

if __name__ == '__main__':
    b_set = EnvironmentSettings\
        .new_instance()\
        .in_batch_mode()\
        .use_blink_planner()\
        .build()
    bt_env = BatchTableEnvironment.create(environment_settings=b_set)
    df = pd.DataFrame({
        '_int': [1, 2],
        '_timestamp': [datetime.now(), pd.Timestamp.now().tz_localize('UTC')],
        '_array': [[1, None]],
        '_row': [{'_id':'hello'}, {'_id':'world'}]
    })
    print(df)

    # Won't work
    # bt_env.from_pandas(df).execute().print()

    # Works!
    from_pandas(bt_env, df).execute().print()    

    
