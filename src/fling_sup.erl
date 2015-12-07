-module(fling_sup).
-behaviour(supervisor).

-export([
	 start_link/0,
	 start_ets_server/3,
	 init/1
	]).

start_link() ->
    supervisor:start_link({local, fling_sup}, ?MODULE, []).

start_ets_server(GetKey, GetValue, ModName) ->
    supervisor:start_child(fling_sup, [GetKey, GetValue, ModName]).

init([]) ->
    SupFlags = {simple_one_for_one, 10, 10},
    ETSServerSpec = {fling_ets_server,
		       {fling_ets, start_link, []},
		       temporary,
		       2000,
		       worker,
		       [fling_ets]},
    {ok, {SupFlags, [ETSServerSpec]}}.
