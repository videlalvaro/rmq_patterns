-module(control_consumer).

-export([start/1, amqp_lifecycle/1]).

-include("amqp_client.hrl").

% P = control_consumer:start(<<"char_count_server.char_count">>).
start(BindKey) ->
    Pid = spawn(control_consumer, amqp_lifecycle, [BindKey]),
    Pid.

amqp_lifecycle(BindKey) ->

    %% Start a *direct* connection to the server
    %% This is the ONLY line of code that is different from 'stocks_example.erl'
    {ok, Connection} = amqp_connection:start(network, #amqp_params{}),

    %% Once you have a connection to the server, you can start an AMQP channel
    {ok, Channel} = amqp_connection:open_channel(Connection),

    %% Now that you have access to a connection with the server, you can declare a queue and bind it to an exchange
    X = <<"control">>,
        
    #'queue.declare_ok'{queue = Q}
        = amqp_channel:call(Channel, #'queue.declare'{exclusive = true, auto_delete = true}),
    log(queue,Q),

    QueueBind = #'queue.bind'{queue = Q,
                              exchange = X,
                              routing_key = BindKey},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),

    %% The queue has now been set up and you have an open channel to so you can do something useful now.
    log(setup_consumer,"start"),
    setup_consumer(Channel, Q),
    log(setup_consumer,"finished"),

    %% After you've finished with the channel and connection you should close them down
    log(channel_close,"start"),
    ok = amqp_channel:close(Channel),

    log(connection_close,"start"),
    ok = amqp_connection:close(Connection),
    log(connection_close,"Demo Completed!"),
    ok.

% send_message(Channel, X, RoutingKey, Payload) ->
%     log(send_message,"basic.publish setup"),
%     BasicPublish = #'basic.publish'{exchange = X, routing_key = RoutingKey},
% 
%     log(send_message,"amqp_channel:cast"),
%     ok = amqp_channel:cast(Channel, BasicPublish, _MsgPayload = #amqp_msg{payload = Payload}).

setup_consumer(Channel, Q) ->

    %% Register a consumer to listen to a queue
    log(setup_consumer,"basic.consume"),
    BasicConsume = #'basic.consume'{queue = Q,
                                    consumer_tag = <<"">>,
                                    no_ack = true},
    #'basic.consume_ok'{consumer_tag = ConsumerTag}
                     = amqp_channel:subscribe(Channel, BasicConsume, self()),

    %% If the registration was sucessful, then consumer will be notified
    log(setup_consumer,"basic.consume_ok start receive"),
    receive
        #'basic.consume_ok'{consumer_tag = ConsumerTag} -> ok
    end,
    log(setup_consumer,"basic.consume_ok finished"),

    %% When a message is routed to the queue, it will then be delivered to this consumer
    log(read_messages,"start"),
    Msg = read_messages(),
    io:format("Msg: ~p~n", [Msg]),
    log(read_messages,"finish"),

    %% After the consumer is finished interacting with the queue, it can deregister itself
    log(basic_cancel,"start"),
    BasicCancel = #'basic.cancel'{consumer_tag = ConsumerTag},
    #'basic.cancel_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(Channel,BasicCancel).

read_messages() ->
    receive
        stop -> ok;
        {#'basic.deliver'{consumer_tag=_ConsumerTag, delivery_tag=_DeliveryTag, redelivered=_Redelivered, exchange=_Exchange, routing_key=RoutingKey}, Content} ->
            log(read_messages,"basic.deliver"),
            io:format("RoutingKey received: ~p~n", [RoutingKey]),
            #amqp_msg{payload = Payload} = Content,
            io:format("Payload received: ~p~n", [binary_to_term(Payload)]),
            read_messages();
        Any ->
            io:format("received unexpected Any: ~p~n", [Any]),
            read_messages()
    end.

log(Key,Value) ->
    io:format("~p: ~p~n",[Key,Value]).
