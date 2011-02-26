-module(rmq_patterns_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    rmq_patterns_sup:start_link().

stop(_State) ->
    ok.
