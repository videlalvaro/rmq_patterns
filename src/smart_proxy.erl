-module(smart_proxy).

-behaviour(gen_server).

-export([start_link/0, consumer_options/0, stop_proxy/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
         
-include("amqp_client.hrl").

-record(state, {connection, channel, consumer_tag, exchange, routing_key, queue_name}).

stop_proxy() ->
    gen_server:call(?MODULE, stop_proxy).
    

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    io:format("Starting rmq_control...~n", []),
    {ok, {Connection, Channel, Queue, ConsumerTag, Exchange, RoutingKey}} = setup_consumer(),
    {ok, #state{connection = Connection,
                channel = Channel,
                queue_name = Queue,
                consumer_tag = ConsumerTag,
                exchange = Exchange,
                routing_key = RoutingKey}}.

%% callbacks
handle_call(stop_proxy, _From, #state{channel = Channel, consumer_tag = Tag} = State) ->
    log("Stoping consumer", "OK"),
    #'basic.cancel_ok'{} =
        amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = Tag}),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};
    
handle_info(#'basic.cancel_ok'{}, State) ->
    {stop, normal, State};

handle_info({#'basic.deliver'{consumer_tag=_ConsumerTag, delivery_tag=_DeliveryTag, 
                              redelivered=_Redelivered, exchange=_Exchange, 
                              routing_key=_RoutingKey}, Content}, State) ->
    #amqp_msg{payload = Payload} = Content,
    log("basic.deliver", Payload),
    {noreply, State};

handle_info(Info, State) ->
    log(Info, State),
    {noreply, State}.

terminate(_Reason, #state{channel = Channel}) ->
    amqp_channel:close(Channel),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
    
setup_consumer() ->
    {ok, Connection} = amqp_connection:start(network, get_record(amqp_params)),
    % {ok, Connection} = amqp_connection:start(network, #amqp_params{}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    
    log("Channel", "OK"),
    
    QDeclare = get_record('queue.declare'),
    #'queue.declare_ok'{} = amqp_channel:call(Channel, QDeclare),
    
    log("QDeclare", "OK"),
    
    ExDeclare = get_record('exchange.declare'),
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExDeclare),
    
    log("ExDeclare", "OK"),
    
    Bind = get_record('queue.bind'),
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Bind),
    
    log("Bind", "OK"),
    
    BasicConsume = get_record('basic.consume'),
    #'basic.consume_ok'{consumer_tag = ConsumerTag}
                     = amqp_channel:subscribe(Channel, BasicConsume, self()),
                     
    log("BasicConsume", "OK"),
                     
    receive
        #'basic.consume_ok'{consumer_tag = ConsumerTag} -> ok
    end,
    
    log("Ready", "OK"),
    
    {ok, {Connection, Channel, 
            QDeclare#'queue.declare'.queue, 
            BasicConsume#'basic.consume'.consumer_tag, 
            ExDeclare#'exchange.declare'.exchange, Bind#'queue.bind'.routing_key}}.
    
consumer_options() ->
    Ex = <<"upload_picture">>,
    Q = <<"smart_proxy">>,
    RK = <<"">>,
    CTag = <<"smart_proxy_consumer">>,
    
    [{amqp_params,
    
        [{host, "localhost"}, {port, 5672}, {username, <<"guest">>}, 
         {password, <<"guest">>}, {virtual_host, <<"/">>}]},
    
    {'queue.declare', 

      [{queue, Q}, {passive, false}, {durable, true}, {exclusive, false}, {auto_delete, false},
      {nowait, false}, {arguments, []}]},

    {'exchange.declare', 

      [{exchange, Ex}, {type, <<"direct">>}, {passive, false}, {durable, true},
      {auto_delete, false}, {internal, false}, {nowait, false}, {arguments, []}]},

    {'queue.bind', 

      [{queue, Q}, {exchange, Ex}, {routing_key, RK}, {nowait, false}, {arguments, []}]},

    {'basic.consume', 

      [{queue, Q}, {consumer_tag, CTag}, {no_local, false}, {no_ack, false},
      {exclusive, false}, {nowait, false}]}].
    
    
get_opt(Call, Key) ->
  Conf = proplists:get_value(Call, consumer_options()),
  proplists:get_value(Key, Conf).

get_record('queue.declare') ->
  #'queue.declare'{ queue = get_opt('queue.declare', queue),
                    passive = get_opt('queue.declare', passive), 
                    durable = get_opt('queue.declare', durable),
                    exclusive = get_opt('queue.declare', exclusive), 
                    auto_delete = get_opt('queue.declare', auto_delete),
                    nowait = get_opt('queue.declare', nowait), 
                    arguments = get_opt('queue.declare', arguments)};

get_record('exchange.declare') ->
   #'exchange.declare'{ exchange = get_opt('exchange.declare', exchange), 
                        type = get_opt('exchange.declare', type),
                        passive = get_opt('exchange.declare', passive), 
                        durable = get_opt('exchange.declare', durable),
                        auto_delete = get_opt('exchange.declare', auto_delete), 
                        internal = get_opt('exchange.declare', internal),
                        nowait = get_opt('exchange.declare', nowait), 
                        arguments = get_opt('exchange.declare', arguments)};

get_record('queue.bind') ->
   #'queue.bind'{ queue = get_opt('queue.bind', queue),
                  exchange = get_opt('queue.bind', exchange),
                  routing_key = get_opt('queue.bind', routing_key),
                  nowait = get_opt('queue.bind', nowait), 
                  arguments = get_opt('queue.bind', arguments)};

get_record('basic.consume') ->
  #'basic.consume'{ queue = get_opt('basic.consume', queue),
                    consumer_tag = get_opt('basic.consume', queue),
                    no_local = get_opt('basic.consume', no_local),
                    no_ack = get_opt('basic.consume', no_ack),
                    exclusive = get_opt('basic.consume', exclusive),
                    nowait = get_opt('basic.consume', nowait)};

get_record(amqp_params) ->
  #amqp_params{username = get_opt(amqp_params, username), 
                password = get_opt(amqp_params, password), 
                host = get_opt(amqp_params, host), 
                port = get_opt(amqp_params, port),
                virtual_host = get_opt(amqp_params, virtual_host)}.

log(Key,Value) ->
    io:format("~p: ~p~n",[Key,Value]).