%% @doc Fling is a cache library that abuses the constant pool of modules for
%% off-heap shared access to arbitrary Erlang terms that is much faster than
%% ETS.  However, it comes at the cost of a dynamically compiled and loaded
%% Erlang module, so this technique is really only suitable for situations
%% with very low write needs and very high read concurrency.
%%
%% Fling takes the approach that the caller is responsible for creating an
%% ETS table as the initial cache. Operation of this cache can proceed as
%% normal - loading data into ETS as usual.  When writes are all or mostly
%% finished, the caller turns the ETS table over to a fling gen_server
%% which manages the state of the cache from then on.
%%
%% When puts to the turned over table have hit a certain threshold of
%% time with no new puts -- by default this is 5 ticks of 5 seconds each,
%% or 25 seconds total -- fling "promotes" the ETS table into a dynamically
%% constructed Erlang module which can then be used in place of the ETS 
%% table for significantly faster lookups.
%%
%% If a new put occurs in the "module" state, the write is accepted in
%% ETS and the timer to promote a new module starts again.
-module(fling).
-export([
	 gen_module_name/0,
	 manage/4,
	 state/1,
	 mode/1,
	 get/4,
	 put/2,
	 put_async/2
	]).

%% @doc This method gives an ETS table away to fling. In general, 
%% `public' and `protected' tables work best. The pid returned is
%% the gen_server managing the state of the given cache.
-spec manage(      Tid :: ets:tid(),
	        GetKey :: fling_mochiglobal:get_expr_fun(),
	      GetValue :: fling_mochiglobal:get_expr_fun(),
	       ModName :: atom() ) -> Pid :: pid().
manage(Tid, GetKey, GetValue, ModName) ->
    {ok, Pid} = fling_sup:start_ets_server(GetKey, GetValue, ModName),
    ets:give_away(Tid, Pid, []),
    Pid.

%% @doc Generate a unique random module name for use when an ETS table
%% is promoted.
-spec gen_module_name() -> atom().
gen_module_name() ->
    fling_mochiglobal:gen_module_name().

-spec state( Pid :: pid() ) -> proplists:proplist().
%% @doc Returns the current state of the cache manager. Keys returned are:
%% <ul>
%% 	<li>`modname' which is the module name if the manager is in `mg' mode</li>
%% 	<li>`mode' which is the current mode - `ets' or `mg'<li>
%% 	<li>`tid' which is the ETS table identifier</li>
%% 	<li>`ticks' which is the current tick count</li>
%% </ul>
state(Pid) ->
    fling_ets:status(Pid).

-spec mode( Pid :: pid() ) -> mg|ets.
%% @doc Returns the current mode of the cache manager. `ets' means the cache
%% is still using ETS for reads and writes. `mg' means the cache is in read-only
%% compiled module mode.
mode(Pid) ->
    proplists:get_value(mode, state(Pid)).

-spec get(mg|ets, Tid :: ets:tid(), ModName :: atom(), Key :: term() ) -> Value :: term() | undefined.
get(mg, _Tid, ModName, Key) -> 
    fling_mochiglobal:get(ModName, Key);
get(ets, Tid, _ModName, Key) ->
    case ets:lookup(Tid, Key) of
	[] -> undefined;
	Value -> Value
    end.

put(Pid, Obj) ->
    fling_ets:put(Pid, Obj).

put_async(Pid, Obj) ->
    fling_ets:put_async(Pid, Obj).
