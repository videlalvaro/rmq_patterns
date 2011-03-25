%% @doc This module implements the Smart Proxy pattern as explained here:
%% http://www.eaipatterns.com/SmartProxy.html
-module(amqp_smart_proxy).

-behaviour(gen_server).

-include("amqp_client.hrl").
-include("rmq_patterns.hrl").

-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).
-export([start/1]).
-export([stop/1]).
-export([stats/1, clear_stats/1]).

-export([start_demo/0, start_demo/1]).

-record(state, {channel,
                control_exchange,
                control_rkey,
                control_ctag,
                proxy_queue,
                proxy_exchange,
                proxy_rkey,
                rpc_exchange,
                rpc_rkey,
                smart_proxy_ctag = <<"">>,
                rpc_ctag = <<"">>,
                correlation_id = make_ref(),
                continuations = dict:new(),
                t_ref,
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
    ControlExchange = <<"control">>,
    ControlRKey = <<"control.smart_proxy">>,
    Pid = amqp_smart_proxy:start([Connection, ControlExchange, ControlRKey, Opts]),
    io:format("Server started with Pid: ~p~n", [Pid]),
    Pid.

start_demo() ->
    demo([]).

start_demo(debug) ->
    demo([{debug, [trace]}]).

start([Connection, ControlExchange, ControlRKey, Opts]) ->
    {ok, Pid} = gen_server:start(?MODULE, [Connection, ControlExchange, ControlRKey], Opts),
    Pid.

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).
    
stats(Pid) ->
    gen_server:call(Pid, stats).

clear_stats(Pid) ->
    gen_server:call(Pid, clear_stats).

%%--------------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------------

init([Connection, ControlExchange, ControlRKey]) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    
    {ok, ControlCTag} = amqp_utils:init_controlled_consumer(Channel, 
                            ControlExchange, ControlRKey),
    
    {ok, #state{channel = Channel, control_exchange = ControlExchange, 
                    control_rkey = ControlRKey, control_ctag = ControlCTag,
                    stats = #stats{}}}.

%% @private
handle_info(shutdown, State) ->
    {stop, normal, State};

%% @private
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

%% @private
%% Stop to gen_server if we stop consuming from the Control Bus
handle_info(#'basic.cancel_ok'{consumer_tag = ControlCTag}, 
                #state{control_ctag = ControlCTag, t_ref = TRef} = State) ->
    timer:cancel(TRef),
    {stop, normal, State#state{stats = #stats{}}};

%% @private
%% Continue working if we stop consuming from a Proxy'ed queue
handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State};

%% Handles a message from the Control Bus to start the Smart Proxy
handle_info({#'basic.deliver'{consumer_tag = ControlCTag},
             #amqp_msg{payload = Msg}},
             #state{channel = Channel, control_ctag = ControlCTag,
                        smart_proxy_ctag = SPCTag, rpc_ctag = RPCCtag,
                        t_ref = TRef} = State) ->
    
    amqp_utils:stop_consumers([SPCTag, RPCCtag], Channel),
    timer:cancel(TRef),
    
    #smart_proxy_msg{in_exchange = InExchange, in_rkey = InRKey, 
         out_exchange = OutExchange, out_rkey = OutRKey, 
         interval = Interval} = binary_to_term(Msg),
    
    %%setup reply queue to get messages from RPC Server.
    #'queue.declare_ok'{queue = ReplyQ} 
        = amqp_channel:call(Channel, #'queue.declare'{exclusive = true, auto_delete = true}),
    #'basic.consume_ok'{consumer_tag = RPCCtag2} = 
        amqp_channel:subscribe(Channel, #'basic.consume'{queue = ReplyQ, no_ack = true}, self()),
    
    %%setup proxy queue to get messages from RPC Client
    #'queue.declare_ok'{queue = ProxyQ} 
        = amqp_channel:call(Channel, #'queue.declare'{exclusive = true, auto_delete = true}),
    QueueBind = #'queue.bind'{queue = ProxyQ,
                              exchange = InExchange},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
    #'basic.consume_ok'{consumer_tag = SPCTag2} = 
        amqp_channel:subscribe(Channel, #'basic.consume'{queue = ProxyQ}, self()),
    
    {ok, TRef2} = timer:send_interval(Interval, flush_stats),
    
    {noreply, State#state{proxy_queue = ProxyQ, proxy_exchange = InExchange, 
                          proxy_rkey = InRKey, rpc_exchange = OutExchange, 
                          rpc_rkey = OutRKey, smart_proxy_ctag = SPCTag2, 
                          rpc_ctag = RPCCtag2, t_ref = TRef2}};

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