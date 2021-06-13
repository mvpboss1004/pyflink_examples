# JSON Input
PyFlink documents provides some code snippets about how to define a json input format. Ideally both `pyflink.table.descriptors.Json` and DDL can create a json-format source. But after a long time dealing with endless bugs, I think I'd best use DDL only.  
Below are some disadvantages of using `pyflink.table.descriptors.Json` combined with `pyflink.table.TableEnvironment.connect()`:
 - You have to define one schema for three times, in [json-schema](http://json-schema.org/)/`pyflink.table.DataTypes`/`pyflink.table.descriptors.Schema` seperately. This can be very confusing.
 - Array of strings is not supported. It will throw exceptions like `Type ARRAY<STRING> of table field tags does not match with the physical type LEGACY('ARRAY', 'ANY<[Ljava.lang.String; ...`
 - You can't define date-time field format.