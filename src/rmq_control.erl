-module(rmq_control).
% 
% -behaviour(gen_server).
% 
% -export([start_link/0]).
% 
% -export([init/1, handle_call/3, handle_cast/2, handle_info/2,
%          terminate/2, code_change/3]).
%          
% -include("amqp_client.hrl").
% 
% -record(state, {connection, channel, consumer_tag, exchange, routing_key, queue_name}).
% 
% start_link() ->
%     gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
% 
% init([]) ->
%     io:format("Starting rmq_control...~n", []),
%     {ok, {Connection, Channel, Queue, ConsumerTag, Exchange, RoutingKey}} = setup_consumer(),
%     {ok, #state{connection = Connection,
%                 channel = Channel,
%                 queue_name = Queue,
%                 consumer_tag = ConsumerTag,
%                 exchange = Exchange,
%                 routing_key = RoutingKey}}.
% 
% %% callbacks
% handle_call(hello, _From, State) ->
%     io:format("Hello from server!~n", []),
%     {reply, ok, State};
% 
% handle_call(_Request, _From, State) ->
%     Reply = ok,
%     {reply, Reply, State}.
% 
% handle_cast(_Msg, State) ->
%     {noreply, State}.
% 
% handle_info({#'basic.deliver'{consumer_tag=_ConsumerTag, delivery_tag=_DeliveryTag, 
%                               redelivered=_Redelivered, exchange=_Exchange, 
%                               routing_key=_RoutingKey}, Content}, State) ->
%     #amqp_msg{payload = Payload} = Content,
%     io:format("~p~n", [Payload]),
%     {noreply, State};
% 
% handle_info(Info, State) ->
%     io:format("~p~n", [Info]),
%     {noreply, State}.
% 
% terminate(_Reason, _State) ->
%     ok.
% 
% code_change(_OldVsn, State, _Extra) ->
%     {ok, State}.
%     
% setup_consumer() ->
%     {ok, Connection} = amqp_connection:start(network, get_record(amqp_params)),
%     {ok, Channel} = amqp_connection:open_channel(Connection),
%     
%     #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, get_record('queue.declare')),
%     
%     Exchange = <<"amq.rabbitmq.log">>,
%     % Declare = #'exchange.declare'{exchange = Exchange,
%     %                               type = <<"topic">>},
%     %                               
%     % -record('exchange.declare', {ticket = 0, exchange, type = <<"direct">>, passive = false, durable = false, auto_delete = false, internal = false, nowait = false, arguments = []}).
%     % #'exchange.declare_ok'{} = amqp_channel:call(Channel, Declare),
%     
%     RoutingKey = <<"#">>,
%     Binding = #'queue.bind'{queue        = Queue,
%                             exchange     = Exchange,
%                              routing_key = RoutingKey},
%     #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
%     
%     ConsumerTag = <<"my_tag">>,
%     BasicConsume = #'basic.consume'{queue = Queue,
%                                     consumer_tag = ConsumerTag,
%                                     no_ack = true},
%     #'basic.consume_ok'{consumer_tag = ConsumerTag}
%                      = amqp_channel:subscribe(Channel, BasicConsume, self()),
%                      
%     receive
%         #'basic.consume_ok'{consumer_tag = ConsumerTag} -> ok
%     end,
%     
%     {ok, {Connection, Channel, Queue, ConsumerTag, Exchange, RoutingKey}}.
%     
% anon_consumer_options() ->
%     [{'queue.declare', 
% 
%       [{queue, <<"">>}, {passive, false}, {durable, false}, {exclusive, true}, {auto_delete, true},
%       {nowait, false}, {arguments, []}]},
% 
%     {'exchange.declare', 
% 
%       [{exchange, <<"">>}, {type, "direct"}, {passive, false}, {durable, true},
%       {auto_delete, false}, {internal, false}, {nowait, false}, {arguments, []}]},
% 
%     {'queue.bind', 
% 
%       [{nowait, false}, {arguments, []}]},
% 
%     {'basic.consume', 
% 
%       [{consumer_tag, "msgs_consumer"}, {no_local, false}, {no_ack, false},
%       {exclusive, false}, {nowait, false}, {ticket, []}]}].
%     
%     
% get_opt(Call, Key) ->
%   {ok, Conf} = application:get_env(Call),
%   proplists:get_value(Key, Conf).
% 
% get_record('queue.declare') ->
%   #'queue.declare'{ queue = get_opt(queue_declare, queue),
%                     passive = get_opt(queue_declare, passive), 
%                     durable = get_opt(queue_declare, durable),
%                     exclusive = get_opt(queue_declare, exclusive), 
%                     auto_delete = get_opt(queue_declare, auto_delete),
%                     nowait = get_opt(queue_declare, nowait), 
%                     arguments = get_opt(queue_declare, arguments)};
% 
% get_record('exchange.declare') ->
%    #'exchange.declare'{ exchange = get_opt(exchange_declare, exchange), 
%                         type = get_opt(exchange_declare, type),
%                         passive = get_opt(exchange_declare, passive), 
%                         durable = get_opt(exchange_declare, durable),
%                         auto_delete = get_opt(exchange_declare, auto_delete), 
%                         internal = get_opt(exchange_declare, internal),
%                         nowait = get_opt(exchange_declare, nowait), 
%                         arguments = get_opt(exchange_declare, arguments)};
% 
% get_record('queue.bind') ->
%    #'queue.bind'{ queue = get_opt(queue_bind, queue),
%                   exchange = get_opt(queue_bind, exchange),
%                   routing_key = get_opt(queue_bind, routing_key),
%                   nowait = get_opt(queue_bind, nowait), 
%                   arguments = get_opt(queue_bind, arguments)};
% 
% get_record('basic.consume') ->
%   #'basic.consume'{ queue = get_opt(basic_consume, queue),
%                     consumer_tag = get_opt(basic_consume, queue),
%                     no_local = get_opt(basic_consume, no_local),
%                     no_ack = get_opt(basic_consume, no_ack),
%                     exclusive = get_opt(basic_consume, exclusive),
%                     nowait = get_opt(basic_consume, nowait)};
% 
% get_record('amqp.params') ->
%   #amqp_params{username=get_opt(connection, user), 
%                 password=get_opt(connection, pass), 
%                 host=get_opt(connection, host), 
%                 port=get_opt(connection, port),
%                 virtual_host=get_opt(connection, virtual_host)}.
% 
% log(Key,Value) ->
%     io:format("~p: ~p~n",[Key,Value]).