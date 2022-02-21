# User Defined Function
## 1. UDAF
According to the help message of `udaf`:
>    Helper method for creating a user-defined aggregate function.
>
>    Example:
>        ::
>
>            >>> # The input_types is optional.
>            >>> @udaf(result_type=DataTypes.FLOAT(), func_type="pandas")
>            ... def mean_udaf(v):
>            ...     return v.mean()
>
>    :param f: user-defined aggregate function.  
>    :param input_types: optional, the input data types.  
>    :param result_type: the result data type.  
>    :param accumulator_type: optional, the accumulator data type.
>    :param deterministic: the determinism of the function's results. True if and only if a call to this function is guaranteed to always return the same result given the same parameters. (default True)  
>    :param name: the function name.  
>    :param func_type: the type of the python function, available value: general, pandas, (default: general)  
>    :return: UserDefinedAggregateFunctionWrapper or function.

Both `accumulator_type` and `func_type` are optional. But if you omit them, you may encounter the following exceptions.
### about `accumulator_type`
The exception message:
>Traceback (most recent call last):  
>  File "udf.py", line 16, in <module>  
>    @udaf(result_type=T.INT())  
>  File "/usr/share/flink-1.13.0/opt/python/pyflink.zip/  pyflink/table/udf.py", line 564, in _create_udaf  
>  File "/usr/share/flink-1.13.0/opt/python/pyflink.zip/pyflink/table/udf.py", line 491, in __init__  
>AttributeError: 'function' object has no attribute 'get_accumulator_type'

So you must define `accumulator_type`.

### about `func_type`
The exception message:
>Traceback (most recent call last):  
>  File "udf.py", line 29, in   
>    bt_env.create_temporary_system_function('BIT_OR_AGGR', bit_or_aggr)  
>  File "/usr/share/flink-1.13.0/opt/python/pyflink.zip/pyflink/table/table_environment.py", line 276, in create_temporary_system_function  
>  File "/usr/share/flink-1.13.0/opt/python/pyflink.zip/pyflink/table/udf.py", line 387, in _java_user_defined_function  
>  File "/usr/share/flink-1.13.0/opt/python/pyflink.zip/pyflink/table/udf.py", line 542, in _create_delegate_function  
>AssertionError  

When I look into the source code [line 542, in _create_delegate_function](https://github.com/apache/flink/blob/release-1.13.0/flink-python/pyflink/table/udf.py), I found:
```
    def _create_delegate_function(self) -> UserDefinedFunction:
        assert self._func_type == 'pandas'
        return DelegatingPandasAggregateFunction(self._func)
```
So, if the default `func_type` is `general`, what is the meaning of this assertion?  
But for simplicity, you can define `func_type='pandas'` and install pandas in your virtual environment.

## How To Use
 - Create a virtual environment: `virtualenv flink_venv`
 - Enable it: `cd flink_venv; source bin/activate`
 - Install dependencies: `pip install apache-flink pandas apache-beam`
 - Zip the environment: `zip -r ../flink_venv.zip ./`
 - Start the application `flink run -t local -py udf.py -pyarch /path/to/flink_venv.zip -pyexec flink_venv.zip/bin/python`

## 2. UDF
When your udf is not a table-mapping function, you need to specific a `DataTypes.ROW()` object as output_type, and return a `Row()` object in your function. But be careful!!! When fields in `DataTypes.ROW()` can be in arbitary order, values in `Row()` are always sorted by field names. Example:  

>In [1]: from pyflink.table import Row
>In [2]: r0 = Row(name='Alice', age=1)
>In [3]: r0._values
>Out[3]: [1, 'Alice']
>In [5]: Person = Row('name', 'age')
>In [7]: Person._values
>Out[7]: ['name', 'age']
>In [8]: r1 = Person('Bob', 2)
>In [9]: r1._values
>Out[9]: ['Bob', 2]

So the best way is always sort your field before create the `DataTypes.ROW()` schema. See `json_load()` function in udf.py:
