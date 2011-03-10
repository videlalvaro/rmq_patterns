%% @doc This module implements the Smart Proxy pattern as explained here:
%% http://www.eaipatterns.com/SmartProxy.html
-module(amqp_smart_proxy).

-behaviour(gen_server).

-include("amqp_client.hrl").

-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).
-export([start/1]).
-export([stop/1]).
-export([stats/1, clear_stats/1]).

-export([start_demo/0, start_demo/1]).

-record(state, {channel,
                proxy_queue,
                proxy_exchange,
                rpc_exchange,
                control_exchange,
                correlation_id = make_ref(),
                continuations = dict:new(),
                stats}).
                
-record(proxied_req, {correlation_id,
                       reply_queue,
                       req_time}).
                       
-record(stats, {total_msgs = 0, time_total = 0, outgoing = 0, min = 0, max = 0}).

%%--------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------

demo(Opts) ->
    {ok, Connection} = amqp_connection:start(network, #amqp_params{}),
    ProxyEx = <<"char_count">>,
    RpcExchange = <<"char_count_server">>,
    ControlExchange = <<"control">>,
    Pid = amqp_smart_proxy:start([Connection, ProxyEx, RpcExchange, ControlExchange, interval_ms(), Opts]),
    io:format("Server started with Pid: ~p~n", [Pid]),
    Pid.

start_demo() ->
    demo([]).

start_demo(debug) ->
    demo([{debug, [trace]}]).
    
interval_ms() -> 60000.


%% @spec (Connection, Queue, RpcHandler) -> RpcServer
%% where
%%      Connection = pid()
%%      ProxyEx = binary()
%%      RpcHandler = binary()
%%      RpcHandler = function()
%%      RpcServer = pid()
%% @doc Starts a new RPC server instance that receives requests via a
%% specified queue and dispatches them to a specified handler function. This
%% function returns the pid of the RPC server that can be used to stop the
%% server.
start([Connection, ProxyEx, RpcExchange, ControlExchange, Interval, Opts]) ->
    {ok, Pid} = gen_server:start(?MODULE, [Connection, ProxyEx, RpcExchange, ControlExchange, Interval], Opts),
    Pid.

%% @spec (RpcServer) -> ok
%% where
%%      RpcServer = pid()
%% @doc Stops an exisiting RPC server.
stop(Pid) ->
    gen_server:call(Pid, stop, infinity).
    
stats(Pid) ->
    gen_server:call(Pid, stats).

clear_stats(Pid) ->
    gen_server:call(Pid, clear_stats).

%%--------------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------------

%% @private
init([Connection, ProxyEx, RpcExchange, ControlExchange, Interval]) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    
    %%setup reply queue to get messages from RPC Server.
    #'queue.declare_ok'{queue = ReplyQ} 
        = amqp_channel:call(Channel, #'queue.declare'{exclusive = true, auto_delete = true}),
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = ReplyQ, no_ack = true}, self()),
    
    %%setup proxy exchange
    ExDeclare = #'exchange.declare'{exchange=ProxyEx, durable = true},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExDeclare),
    
    %%setup proxy queue to get messages from RPC Client
    #'queue.declare_ok'{queue = ProxyQ} 
        = amqp_channel:call(Channel, #'queue.declare'{exclusive = true, auto_delete = true}),
    QueueBind = #'queue.bind'{queue = ProxyQ,
                              exchange = ProxyEx},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = ProxyQ}, self()),
    
    timer:send_interval(Interval, flush_stats),
    
    {ok, #state{channel = Channel, proxy_queue = ReplyQ, 
        proxy_exchange = ProxyEx, rpc_exchange = RpcExchange, 
        control_exchange = ControlExchange, stats = #stats{}}}.

%% @private
handle_info(shutdown, State) ->
    {stop, normal, State};

%% @private
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

%% @private
handle_info(#'basic.cancel_ok'{}, State) ->
    {stop, normal, State};

%% @private
%% handles message from the RPC Client and forwards to the RPC server
handle_info({#'basic.deliver'{delivery_tag = DeliveryTag, exchange = ProxyEx},
             #amqp_msg{props = Props} = Msg},
             
             #state{channel = Channel, proxy_queue = ProxyQ,
                proxy_exchange = ProxyEx, rpc_exchange = RpcExchange,
                correlation_id = CorrelationId, continuations = Continuations,
                stats = Stats} = State) ->
    
    %%extract original message props and store for later
    #'P_basic'{correlation_id = ClientCorrelationId,
               reply_to = ClientQ} = Props,
    
    %% add our own correlation_id and reply_to fields
    Properties = Props#'P_basic'{correlation_id = term_to_binary(CorrelationId), reply_to = ProxyQ},
    Publish = #'basic.publish'{exchange = RpcExchange},
    
    Now = date_utils:now_to_milliseconds_hires(erlang:now()),
    
    amqp_channel:call(Channel, Publish, Msg#amqp_msg{props = Properties}),
    amqp_channel:call(Channel, #'basic.ack'{delivery_tag = DeliveryTag}),
    
    ProxiedReq = #proxied_req{correlation_id = ClientCorrelationId, 
                    reply_queue = ClientQ, req_time = Now},
    
    NewStats = Stats#stats{total_msgs = Stats#stats.total_msgs + 1},
    
    NewState = State#state{correlation_id = make_ref(), 
        continuations = dict:store(CorrelationId, ProxiedReq, Continuations),
        stats = NewStats},
    
    {noreply, NewState};

%% @private
%% handles message from the RPC server and sends it back to the client
handle_info({#'basic.deliver'{},
             #amqp_msg{props = Props} = Msg},
             #state{channel = Channel, continuations = Continuations, 
                    stats = Stats} = State) ->
    
    Now = date_utils:now_to_milliseconds_hires(erlang:now()),
    
    #'P_basic'{correlation_id = CorrelationId} = Props,
    
    #proxied_req{correlation_id = ClientCorrelationId, reply_queue = ClientQ, 
        req_time = ReqTime} = dict:fetch(binary_to_term(CorrelationId), Continuations),
    
    Properties = Props#'P_basic'{correlation_id = ClientCorrelationId},
    Publish = #'basic.publish'{exchange = <<>>,
                               routing_key = ClientQ},
    amqp_channel:call(Channel, Publish, Msg#amqp_msg{props = Properties}),
    
    Cont = dict:erase(CorrelationId, Continuations),
    Elapsed = Now - ReqTime,
    NewStats = Stats#stats{time_total = Stats#stats.time_total + Elapsed,
                    outgoing = dict:size(Cont),
                    min = calc_min(Elapsed, Stats#stats.min),
                    max = calc_max(Elapsed, Stats#stats.max)},
    
    {noreply, State#state{stats = NewStats, continuations = Cont}};

handle_info(flush_stats, #state{channel = Channel, stats = Stats, 
                proxy_exchange = ProxyEx, rpc_exchange = RpcExchange,
                control_exchange = ControlExchange} = State) ->
    Props = #'P_basic'{content_type = <<"application/octet-stream">>},
    
    Payload = [{total_msgs,Stats#stats.total_msgs}, {time_total, Stats#stats.time_total}, 
                {outgoing, Stats#stats.outgoing}, {min, Stats#stats.min}, {max, Stats#stats.max}],
    
    amqp_channel:call(Channel, #'basic.publish'{exchange = ControlExchange, 
        routing_key = <<RpcExchange/binary, ".", ProxyEx/binary>>}, 
            #amqp_msg{props = Props, payload = term_to_binary(Payload)}),
    
    {noreply, State#state{stats = #stats{}}}.

handle_call(stats, _From,  #state{stats = Stats} = State) ->
    {reply, Stats, State};

%% @private
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

%%--------------------------------------------------------------------------
%% Rest of the gen_server callbacks
%%--------------------------------------------------------------------------

%% @private
handle_cast(_Message, State) ->
    {noreply, State}.

%% Closes the channel this gen_server instance started
%% @private
terminate(_Reason, #state{channel = Channel}) ->
    amqp_channel:close(Channel),
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    State.
    
calc_min(Time, 0) ->
    Time;
calc_min(Time, Min) ->
    case Min > Time of
        true -> Time;
        _ -> Min
    end.
    
calc_max(Time, 0) ->
    Time;
calc_max(Time, Max) ->
    case Max < Time of
        true -> Time;
        _ -> Max
    end.