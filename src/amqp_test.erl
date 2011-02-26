-module(amqp_test).

-export([start/0, start_debug/0]).

-include("amqp_client.hrl").

start(Opts) ->
    {ok, Connection} = amqp_connection:start(network, #amqp_params{}),
    ProxyEx = <<"char_count">>,
    RpcExchange = <<"char_count_server">>,
    Fun = undefined,
    amqp_smart_proxy:start(Connection, ProxyEx, RpcExchange, Fun, Opts).

start() ->
    start([]).

start_debug() ->
    start([{debug, [trace]}]).