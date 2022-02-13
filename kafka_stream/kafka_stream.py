import sys

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer

if __name__ == '__main__':
    e_env = StreamExecutionEnvironment.get_execution_environment()
    consumer = FlinkKafkaConsumer(sys.argv[2], SimpleStringSchema(), properties={
        'bootstrap.servers': sys.argv[1],
        'auto.offset.reset': 'latest',
    })
    e_env.add_source(consumer).print()
    e_env.execute()
