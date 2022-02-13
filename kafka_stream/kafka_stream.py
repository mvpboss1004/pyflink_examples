import sys

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer

if __name__ == '__main__':
    e_env = StreamExecutionEnvironment.get_execution_environment()
    consumer = FlinkKafkaConsumer(sys.argv[2], SimpleStringSchema(), properties={
        'properties.bootstrap.servers': sys.argv[1],
        'scan.startup.mode': 'latest-offset',
    })
    stream = e_env\
        .from_source(consumer, WatermarkStrategy.for_monotonous_timestamps())\
        .print()
