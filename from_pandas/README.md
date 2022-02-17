# From Pandas
There are many bugs when creating Table from python elements or pandas dataframe. For example, such simple codes
```
bt_env.from_elements([(1,datetime.now())], ('id','dt')).execute().print()
```
throws the following exception:
>TypeError: The precision must be 3 for LocalZonedTimestampType, got LocalZonedTimestampType(6, true)
