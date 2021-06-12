# Socket Stream
`socketTextStream` is very useful when developing your flink appilcation. But it hasn't been officialy supported in pyflink yet. Anyway, you can easily create it from py4j object of `StreamExecutionEnviron`.

How to use:
 - Run `nc -lk 9999` to start a socket input.
 - Run `flink run -t local -py socket_stream.py localhost 9999` to start this application.
 - Connect the socket, input some words, you'll see it printed in the flink's stdout.
