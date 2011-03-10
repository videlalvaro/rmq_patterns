-module(amqp_utils).

-export([init_controlled_consumer/3]).

init_controlled_consumer(Channel, ControlExchange, ControlRKey) ->
    #'queue.declare_ok'{queue = ControlQ} 
        = amqp_channel:call(Channel, #'queue.declare'{exclusive = true, auto_delete = true}),
    
    QueueBind = #'queue.bind'{queue = ControlQ, exchange = ControlExchange,
                                routing_key = ControlRKey},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
    #'basic.consume_ok'{consumer_tag = ControlCTag} = 
        amqp_channel:subscribe(Channel, #'basic.consume'{queue = ControlQ, no_ack = true}, self()),
    
    {ok, ControlCTag}.