#!/bin/sh

# ./start-dev.php <config_file_name>

cd `dirname $0`
exec erl -pa $PWD/ebin -pa $PWD/deps/amqp_client/include/rabbit_common/ebin \
-pa $PWD/deps/amqp_client/ebin  \
-sname amqp_consumer \
-s rmq_patterns -boot start_sasl