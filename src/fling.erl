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
	 start/0,
	 gen_module_name/0,
	 manage/4,
	 state/1,
	 mode_sync/1,
	 mode/2,
	 get/2,
	 put/2,
	 put_async/2
	]).

-type fling_mode() :: { ets, Tid :: ets:tid() } | { mg, ModName :: atom() }.

start() ->
    application:ensure_all_started(fling),
    application:start(fling).

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
%%
%% <B>Important</B>: This calls into the cache manager gen_server.
state(Pid) ->
    fling_ets:status(Pid).

-spec mode_sync( Pid :: pid() ) -> fling_mode().
%% @doc Returns the current mode of the cache manager. `ets' means the cache
%% is using ETS for reads and writes. `mg' means the cache is in read-only
%% compiled module mode.
%% 
%% <B>Important</B>: This calls into the cache manager gen_server.
mode_sync(Pid) ->
    S = state(Pid),
    case proplists:get_value(mode, S, ets) of
	ets ->
	    {ets, proplists:get_value(tid, S)};
	mg ->
	    {mg, proplists:get_value(modname, S)}
    end.

-spec mode(Tid :: ets:tid(), ModName :: atom()) -> Mode :: fling_mode().
%% @doc Returns the current mode of the cache. Does not call into a gen_server.
mode(Tid, ModName) ->
    fling_mochiglobal:mode(ModName, Tid).

-spec get( Mode :: fling_mode(), Key :: term() ) -> Value :: term() | undefined.
%% @doc Get a value from the cache. Does not call into the cache manager.
get({mg, ModName}, Key) -> 
    fling_mochiglobal:get(ModName, Key);
get({ets, Tid}, Key) ->
    case ets:lookup(Tid, Key) of
	[] -> undefined;
	Value -> Value
    end.

-spec put( Pid :: pid(), Obj :: tuple() | [ tuple() ] ) -> ok.
%% @doc Store a value in the cache. When you store a value, 
%% the cache manager resets its tick count for promotion purposes.
%% 
%% If a put is processed while the cache is in compiled module
%% mode, the put will revert the mode back to ets and start a
%% new timer.
put(Pid, Obj) ->
    fling_ets:put(Pid, Obj).

-spec put_async( Pid :: pid(), Obj :: tuple() | [ tuple() ] ) -> ok.
%% @doc Asynchronously store a value in the cache. When you store a
%% value, the cache manager resets its tick count for promotion purposes.
%%
%% If a put is processed while the cache is in compiled module
%% mode, the put will revert the mode back to ets and start a
%% new timer.
put_async(Pid, Obj) ->
    fling_ets:put_async(Pid, Obj).
