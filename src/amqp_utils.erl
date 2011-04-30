-module(amqp_utils).

-export([init_controlled_consumer/3, stop_consumer/2, stop_consumers/2]).
-export([send_msg/3]).

-include("amqp_client.hrl").

init_controlled_consumer(Channel, ControlExchange, ControlRKey) ->

    #'queue.declare_ok'{queue = ControlQ}
        = amqp_channel:call(Channel, #'queue.declare'{exclusive = true, auto_delete = true}),

    QueueBind = #'queue.bind'{queue = ControlQ, exchange = ControlExchange,
                                routing_key = ControlRKey},

    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),

    #'basic.consume_ok'{consumer_tag = ControlCTag} =
        amqp_channel:subscribe(Channel, #'basic.consume'{queue = ControlQ, no_ack = true}, self()),

    {ok, ControlCTag}.

stop_consumer(Consumer, Channel) ->
    stop_consumers([Consumer], Channel).

stop_consumers([], _Channel) ->
    ok;
stop_consumers([CTag|T], Channel) ->
    case CTag of
        <<"">> -> ok;
        _ ->
            #'basic.cancel_ok'{} =
                amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = CTag})
    end,
    stop_consumers(T, Channel).

%% amqp_utils:send_msg(<<"my_exchange">>, <<"Some Message">>, <<"consumer.key">>).
send_msg(Exchange, Msg, RKey) ->
    {ok, Connection} = amqp_connection:start(network, #amqp_params{}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Publish = #'basic.publish'{exchange = Exchange, routing_key = RKey},
    amqp_channel:call(Channel, Publish, #amqp_msg{payload = Msg}),
    ok.