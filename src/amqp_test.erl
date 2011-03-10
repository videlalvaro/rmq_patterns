-module(amqp_test).

-export([start/0, start_debug/0]).

-include("amqp_client.hrl").

start(Opts) ->
    {ok, Connection} = amqp_connection:start(network, #amqp_params{}),
    ProxyEx = <<"char_count">>,
    RpcExchange = <<"char_count_server">>,
    ControlExchange = <<"control">>,
    Pid = amqp_smart_proxy:start([Connection, ProxyEx, RpcExchange, ControlExchange, interval_ms(), Opts]),
    io:format("Server started with Pid: ~p~n", [Pid]),
    Pid.

start() ->
    start([]).

start_debug() ->
    start([{debug, [trace]}]).
    
interval_ms() -> 60000.