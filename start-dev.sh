#!/bin/sh

cd `dirname $0`
exec erl -pa $PWD/ebin -pa $PWD/deps/*/ebin \
-sname rmq_patterns_$1 \