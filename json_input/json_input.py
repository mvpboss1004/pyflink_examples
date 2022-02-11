import sys
from pyflink.table import EnvironmentSettings, TableEnvironment

if __name__ == '__main__':
    s_set = EnvironmentSettings.in_streaming_mode()
    st_env = TableEnvironment.create(environment_settings=s_set)
    
    # These codes won't work.
    '''
    json_schema = { 
        'type': 'object',
        'properties': {
            'id': {
                'type': 'number'
            },
            'name': {
                'type': 'string'
            },
            'birthday': {
                'type': 'string',
                'format': 'date-time'
            },
            'tags': {
                'type': 'array',
                'items': {
                    'type': 'string'
                }
            }                       
        }
    }
    row_schema = T.ROW([
        T.FIELD('id', T.INT()),
        T.FIELD('name', T.STRING()),
        T.FIELD('birthday', T.TIMESTAMP(3)),
        T.FIELD('tags', T.ARRAY(T.STRING()))
    ])
    format = Json()\
        .json_schema(json.dumps(json_schema))\
        .schema(row_schema)\
        .fail_on_missing_field(False)\
        .ignore_parse_errors(True)
    schema = Schema(fields={
        'id': 'INT',
        'name': 'STRING',
        'birthday': 'TIMESTAMP(3)',
        'tags': 'ARRAY<STRING>'
    })
    kafka = Kafka()\
        .version('universal')\
        .property('bootstrap.servers', sys.argv[1])\
        .topic(sys.argv[2])
    st_env\
        .connect(kafka)\
        .with_format(format)\
        .with_schema(schema)\
        .create_temporary_table('json_input')
    '''

    # These codes work.
    st_env.execute_sql(f'''
        CREATE TABLE json_input (
            `id` INT,
            `name` STRING,
            `birthday` TIMESTAMP(3),
            `tags` ARRAY<STRING>
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{sys.argv[1]}',
            'topic' = '{sys.argv[2]}',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true',
            'json.timestamp-format.standard' = 'SQL'
        )'''
    )
    st_env.from_path('json_input').execute().print()
