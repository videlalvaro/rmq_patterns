-module(rmq_patterns).
-export([start/0, stop/0]).

start() ->
    application:start(rmq_patterns).

stop() ->
    application:stop(rmq_patterns).