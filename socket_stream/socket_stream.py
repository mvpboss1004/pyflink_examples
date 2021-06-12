import sys
from pyflink.datastream import DataStream, StreamExecutionEnvironment

if __name__ == '__main__':
    s_env = StreamExecutionEnvironment.get_execution_environment() 
    socket_stream = DataStream(s_env._j_stream_execution_environment.socketTextStream(sys.argv[1], int(sys.argv[2])))
    socket_stream.print()
    s_env.execute('socket_stream')

