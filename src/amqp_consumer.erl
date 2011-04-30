-module(amqp_consumer).

-behaviour(gen_server).

-include("amqp_client.hrl").
-include("rmq_patterns.hrl").

-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).
-export([start_link/1, start/1]).
-export([stop/1]).

-export([start_demo/2]).

-record(state, {channel,
                control_ctag,
                in_exchange,
                in_rkey,
                consumer_ctag = <<"">>,
                callback}).

%% amqp_consumer:start_demo(<<"control">>, <<"my.rkey">>).
start_demo(Exchange, RKey) ->
  {ok, Connection} = amqp_connection:start(network, #amqp_params{}),
  ?MODULE:start([Connection, Exchange, RKey, []]).

start([Connection, ControlExchange, ControlRKey, Opts]) ->
    gen_server:start(?MODULE, [Connection, ControlExchange, ControlRKey], Opts).

start_link([Connection, ControlExchange, ControlRKey, Opts]) ->
    gen_server:start_link(?MODULE, [Connection, ControlExchange, ControlRKey], Opts).

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

init([Connection, ControlExchange, ControlRKey]) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),

    {ok, ControlCTag} = amqp_utils:init_controlled_consumer(Channel,
                            ControlExchange, ControlRKey),

    {ok, #state{channel = Channel, control_ctag = ControlCTag}}.

handle_info(shutdown, State) ->
    {stop, normal, State};

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

%% Stop to gen_server if we stop consuming from the Control Bus
handle_info(#'basic.cancel_ok'{consumer_tag = ControlCTag},
                #state{control_ctag = ControlCTag} = State) ->
    {stop, normal, State};

%% Continue working if we stop consuming from a Detour Queue
handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State};

%% Handles a message from the Control Bus to start consuming
handle_info({#'basic.deliver'{consumer_tag = ControlCTag},
             #amqp_msg{payload = Msg}},
             #state{channel = Channel, control_ctag = ControlCTag,
                        consumer_ctag = ConsumerCTag} = State) ->

    amqp_utils:stop_consumer(ConsumerCTag, Channel),

    #consumer_msg{in_exchange = InExchange, in_rkey = InRKey,
    callback = CallBack} = binary_to_term(Msg),

    #'queue.declare_ok'{queue = ConsumerQ}
        = amqp_channel:call(Channel, #'queue.declare'{exclusive = true, auto_delete = true}),
    QueueBind = #'queue.bind'{queue = ConsumerQ, exchange = InExchange,
                                routing_key = InRKey},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
    #'basic.consume_ok'{consumer_tag = ConsumerCTag2} =
        amqp_channel:subscribe(Channel, #'basic.consume'{queue = ConsumerQ, no_ack = true}, self()),

    {noreply, State#state{in_exchange = InExchange, in_rkey = InRKey, consumer_ctag = ConsumerCTag2,
                            callback=CallBack}};

%% consume message by executing CallBack.
handle_info({#'basic.deliver'{consumer_tag = ConsumerCTag, exchange = InExchange}, Msg},
             #state{channel = Channel, consumer_ctag = ConsumerCTag,
                    in_exchange = InExchange, callback = CallBack} = State) ->
    CallBack(Channel, Msg),
    {noreply, State}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(_Message, State) ->
    {noreply, State}.

%% Closes the channel this gen_server instance started
terminate(_Reason, #state{channel = Channel}) ->
    amqp_channel:close(Channel),
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.