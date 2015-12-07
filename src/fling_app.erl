-module(fling_app).
-behaviour(application).

-export([
	 start/2,
	 stop/1
	]).

start(_App, _Type) ->
    fling_sup:start_link().

stop(_) -> ok.
