%% @doc This is a utility module that is used to expose an arbitrary function
%% via an asynchronous RPC over AMQP mechanism. It frees the implementor of
%% a simple function from having to plumb this into AMQP. Note that the 
%% RPC server does not handle any data encoding, so it is up to the callback
%% function to marshall and unmarshall message payloads accordingly.
-module(amqp_smart_proxy).

-behaviour(gen_server).

-include("amqp_client.hrl").

-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).
-export([start/5]).
-export([stop/1]).
-export([stats/1]).

-record(state, {channel,
                handler,
                proxy_queue,
                proxy_exchange,
                rpc_exchange,
                correlation_id = 0,
                continuations = dict:new(),
                stats}).
                
-record(proxied_req, {correlation_id,
                       reply_queue,
                       req_time}).
                       
-record(stats, {msgs_nb = 0,
                time_total = 0}).

%%--------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------

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
start(Connection, ProxyEx, RpcExchange, Fun, Opts) ->
    {ok, Pid} = gen_server:start(?MODULE, [Connection, ProxyEx, RpcExchange, Fun], Opts),
    Pid.

%% @spec (RpcServer) -> ok
%% where
%%      RpcServer = pid()
%% @doc Stops an exisiting RPC server.
stop(Pid) ->
    gen_server:call(Pid, stop, infinity).
    
stats(Pid) ->
    gen_server:call(Pid, stats).

%%--------------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------------

%% @private
init([Connection, ProxyEx, RpcExchange, Fun]) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    
    %%setup reply queue to get messages from RPC Server.
    #'queue.declare_ok'{queue = ReplyQ} 
        = amqp_channel:call(Channel, #'queue.declare'{}),
    log("queue.declare", "ok"),
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = ReplyQ}, self()),
    log("basic.consume", "ok"),
    
    %%setup proxy exchange
    ExDeclare = #'exchange.declare'{exchange=ProxyEx, durable = true},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExDeclare),
    
    %%setup proxy queue to get messages from RPC Client
    #'queue.declare_ok'{queue = ProxyQ} 
        = amqp_channel:call(Channel, #'queue.declare'{}),
    log("queue.declare", "ok2"),
    QueueBind = #'queue.bind'{queue = ProxyQ,
                              exchange = ProxyEx},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
    log("queue.bind", "ok"),
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = ProxyQ}, self()),
    log("subscribe", "ok"),
    
    {ok, #state{channel = Channel, handler = Fun, proxy_queue = ReplyQ, 
        proxy_exchange = ProxyEx, rpc_exchange = RpcExchange, stats = #stats{}}}.

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
             #state{handler = _Fun, channel = Channel, proxy_queue = ProxyQ,
                proxy_exchange = ProxyEx, rpc_exchange = RpcExchange,
                correlation_id = CorrelationId, continuations = Continuations,
                stats = Stats} = State) ->
    
    %%extract original message props and store for later
    #'P_basic'{correlation_id = ClientCorrelationId,
               reply_to = ClientQ} = Props,
    
    %% add our own correlation_id and reply_to fields
    Properties = Props#'P_basic'{correlation_id = <<CorrelationId:64>>, reply_to = ProxyQ},
    
    Publish = #'basic.publish'{exchange = RpcExchange},
    
    Now = date_utils:now_to_milliseconds_hires(erlang:now()),
    
    amqp_channel:call(Channel, Publish, Msg#amqp_msg{props = Properties}),
    amqp_channel:call(Channel, #'basic.ack'{delivery_tag = DeliveryTag}),
    
    ProxiedReq = #proxied_req{correlation_id = ClientCorrelationId, 
                    reply_queue = ClientQ, req_time = Now},
    
    NewStats = Stats#stats{msgs_nb = Stats#stats.msgs_nb + 1},
    
    NewState = State#state{correlation_id = CorrelationId + 1, 
        continuations = dict:store(CorrelationId, ProxiedReq, Continuations),
        stats = NewStats},
    
    {noreply, NewState};

%% @private
%% handles message from the RPC server and sends it back to the client
handle_info({#'basic.deliver'{delivery_tag = DeliveryTag},
             #amqp_msg{props = Props} = Msg},
             #state{handler = _Fun, channel = Channel,
                continuations = Continuations, stats = Stats} = State) ->
    
    Now = date_utils:now_to_milliseconds_hires(erlang:now()),
    
    #'P_basic'{correlation_id = <<CorrelationId:64>>} = Props,
    
    #proxied_req{correlation_id = ClientCorrelationId, reply_queue = ClientQ, 
        req_time = ReqTime} = dict:fetch(CorrelationId, Continuations),
    
    Cont = dict:erase(CorrelationId, Continuations),
    
    Properties = Props#'P_basic'{correlation_id = ClientCorrelationId},
    Publish = #'basic.publish'{exchange = <<>>,
                               routing_key = ClientQ},
    
    amqp_channel:call(Channel, Publish, Msg#amqp_msg{props = Properties}),
    amqp_channel:call(Channel, #'basic.ack'{delivery_tag = DeliveryTag}),
    
    Elapsed = Now - ReqTime,
    
    log("Now", Now),
    log("ReqTime", ReqTime),
    log("Elapsed", Elapsed),
    log("time_total", Stats#stats.time_total),
    
    NewStats = Stats#stats{time_total = Stats#stats.time_total + Elapsed},
    
    {noreply, State#state{stats = NewStats, continuations = Cont}}.

handle_call(stats, _From,  #state{stats = Stats} = State) ->
    io:format("Stats ~p~n", [Stats]),
    {reply, ok, State};

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

log(Key,Value) ->
    io:format("~p: ~p~n",[Key,Value]).