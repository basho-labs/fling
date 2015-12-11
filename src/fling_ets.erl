%% @doc Use ETS as our layer to accept writes and after a period of time,
%% quiesce into a mochiglobal object.
%%
%% <B>Note</B>: We're not starting any ETS tables here. Our strategy
%% instead is to accept an ETS "give away" and at that point manage the
%% table.  That way a caller can set up ETS and load it however 
%% she wants to handle that. Until the giveaway, all operations
%% are no-ops.
%%
%% We are using a gen_server here to serialize writes.  In general, gets 
%% should not hit this layer unless there's some kind of linerizability
%% requirement.
%%
%% By default we do 5 ticks of 5 seconds each, we will promote the current
%% state of ETS into a mochiglobal object.
-module(fling_ets).
-behaviour(gen_server).

-export([
   start_link/2,
   start_link/3,
   get/2,
   put/2,
   put_async/2,
   promote/1,
   status/1
	]).

% gen_server callbacks
-export([
	 init/1,
	 terminate/2,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 code_change/3
	]).

-record(state, {
   tid           :: undefined | ets:tid(),
   mode = ets    :: ets | mg,
   modname       :: undefindd | atom(),
   get_key_fun   :: fling_mochiglobal:get_expr_fun(),
   get_value_fun :: fling_mochiglobal:get_expr_fun(),
   pip = false   :: boolean(), %% 'pip' = promotion in progress
   ticks = 0     :: non_neg_integer(),
   max_ticks = 5 :: pos_integer(),
   secs_per_tick :: pos_integer(),
   tref          :: undefined | reference()
}).

-define(TIMEOUT, 10 * 1000).
-define(TICK, fling_tick).

%% public API
%% @doc Start a cache manager, and generate a unique module name.
start_link(GetKey, GetValue) ->
   start_link(GetKey, GetValue, undefined).

%% @doc Start a cache manager using the given values.
start_link(GetKey, GetValue, ModName) ->
   gen_server:start_link(?MODULE, [GetKey, GetValue, ModName], []).

%% @doc Synchronously get a value from the cache.
%%
%% Typically, gets ought to be done directly against the ETS table that backs
%% the cache so they do not go through the gen_server.
get(Pid, Key) ->
   gen_server:call(Pid, {get, Key}, ?TIMEOUT).

%% @doc Perform a synchronous write into the cache.
put(Pid, Obj) ->
   gen_server:call(Pid, {put, Obj}, ?TIMEOUT).

%% @doc Perform asynchronous write into the cache.
put_async(Pid, Obj) ->
   gen_server:cast(Pid, {put, Obj}).

%% @doc Attempt to promote the cache into a module even if the tick timer
%% hasn't expired.
promote(Pid) ->
   gen_server:cast(Pid, {promote}).

%% @doc Synchronously get the current cache state.
status(Pid) ->
   gen_server:call(Pid, {status}, ?TIMEOUT).

%% gen_server callbacks
%% @private
init([GetKey, GetValue, ModName]) ->
   Mod = case ModName of
	    undefined ->
	       fling_mochiglobal:gen_module_name();
	    _ -> ModName
	 end,
   MaxTicks = get_env(max_ticks, 5),
   TickSecs = get_env(secs_per_tick, 5),

   State = #state{ 
	      mode = ets,
	      secs_per_tick = TickSecs * 1000,
	      max_ticks = MaxTicks,
	      modname = Mod,
	      get_key_fun = GetKey,
	      get_value_fun = GetValue },
   {ok, State}.

%% @private
handle_call(Req, _From, State = #state{ tid = undefined }) ->
   lager:warning("Got call ~p but ETS table has not been given away yet.",
		 [Req]),
   {noreply, State};

handle_call({status}, _From, State = #state{ modname = Mod, mode = M, tid = Tid, ticks = T }) ->
   {reply, [{modname, Mod}, {mode, M}, {tid, Tid}, {ticks, T}], State};

handle_call({get, Key}, _From, State = #state{ tid = Tid }) ->
   {reply, ets:lookup(Tid, Key), State};

%% puts without a timer running
handle_call({put, Obj}, _From, State = #state{ mode = ets, 
					       tref = undefined, secs_per_tick = S,
					       tid = Tid }) ->
   Tref = schedule_tick(S),
   {reply, ets:insert(Tid, Obj), State#state{ ticks = 0, tref = Tref }};
handle_call({put, Obj}, _From, State = #state{ modname = M, tref = undefined, 
					       secs_per_tick = S,
					       mode = mg, tid = Tid }) ->
   fling_mochiglobal:purge(M),
   Tref = schedule_tick(S),
   {reply, ets:insert(Tid, Obj), State#state{ ticks = 0, tref = Tref, mode = ets }};

%% puts with a timer running (don't start a new one)
handle_call({put, Obj}, _From, State = #state{ mode = ets, tid = Tid }) ->
   {reply, ets:insert(Tid, Obj), State#state{ ticks = 0 }};
handle_call({put, Obj}, _From, State = #state{ modname = M, mode = mg, tid = Tid }) ->
   fling_mochiglobal:purge(M),
   {reply, ets:insert(Tid, Obj), State#state{ ticks = 0, mode = ets }};

handle_call(Req, _From, State) ->
   lager:error("Unknown call ~p.", [Req]),
   {reply, whoa, State}.

%% @private
handle_cast(Req, State = #state{ tid = undefined }) ->
   lager:warning("Got cast ~p but ETS table has not been given away yet.",
		 [Req]),
   {noreply, State};
handle_cast({promote}, State = #state{ pip = true }) ->
   {noreply, State};
handle_cast({promote}, State = #state{ modname = M, mode = ets, 
				       tid = Tid, get_key_fun = GK,
				       get_value_fun = GV
				     }) ->
   promote(Tid, M, GK, GV),
   {noreply, State#state{ pip = true, tref = undefined }};
%% puts without a timer ref, so start our tick timer
handle_cast({put, Obj}, State = #state{ mode = ets, tid = Tid, 
					tref = undefined, secs_per_tick = S }) ->
   ets:insert(Tid, Obj),
   Tref = schedule_tick(S),
   {noreply, State#state{ ticks = 0, tref = Tref }};
handle_cast({put, Obj}, State = #state{ mode = mg, modname = M, 
					tid = Tid, tref = undefined, 
					secs_per_tick = S }) ->
   fling_mochiglobal:purge(M),
   ets:insert(Tid, Obj),
   Tref = schedule_tick(S),
   {noreply, State#state{ mode = ets, tref = Tref, ticks = 0 }};
%% puts with a timer ref already in our state
handle_cast({put, Obj}, State = #state{ mode = ets, tid = Tid }) ->
   ets:insert(Tid, Obj),
   {noreply, State#state{ ticks = 0 }};
handle_cast({put, Obj}, State = #state{ mode = mg, modname = M, tid = Tid }) ->
   fling_mochiglobal:purge(M),
   ets:insert(Tid, Obj),
   {noreply, State#state{ mode = ets, ticks = 0 }};
handle_cast(Req, State) ->
   lager:warning("Unknown cast ~p", [Req]),
   {noreply, State}.

%% @private
handle_info({'ETS-TRANSFER', Tid, From, Options}, State = #state{ secs_per_tick = S, 
								  tid = undefined }) ->
   lager:debug("Got ETS table ~p from ~p with options ~p", [Tid, From, Options]),
   Tref = schedule_tick(S),
   {noreply, State#state{ tid = Tid, tref = Tref }};
handle_info({'ETS-TRANSFER', Tid, From, _Options}, State = #state{ tid = Current }) ->
   %% This clause should never be executed but...
   lager:error("ETS transfer request from ~p for ETS table ~p but I already have ETS table ~p!",
	       [From, Tid, Current]),
   {noreply, State};

handle_info(?TICK, State = #state{ mode = ets, ticks = T, max_ticks = M, 
					secs_per_tick = S }) when T < M ->
   Tref = schedule_tick(S),
   {noreply, State#state{ ticks = T + 1, tref = Tref }};
handle_info(?TICK, State = #state{ mode = ets, ticks = M, max_ticks = M,
				   modname = Mod, tid = Tid,
				   get_key_fun = GK,
				   get_value_fun = GV,
				   pip = false
				 }) ->
   promote(Tid, Mod, GK, GV),
   {noreply, State#state{ pip = true, tref = undefined }};
handle_info(?TICK, State = #state{ mode = mg }) ->
   lager:debug("Got a tick while in mochiglobal mode.", []),
   {noreply, State#state{ tref = undefined }};
handle_info(?TICK, State = #state{ pip = true }) ->
   lager:debug("Got a tick while promotion in progress.", []),
   {noreply, State#state{ tref = undefined }};
handle_info({'DOWN', _Mref, process, Pid, normal}, State = #state{ modname = ModName }) ->
   lager:debug("Promotion of module ~p completed when ~p exited normally.", 
	       [ModName, Pid]),
   {noreply, State#state{ pip = false, mode = mg }};
handle_info({'DOWN', _Mref, process, Pid, Error}, State = #state{ modname = ModName }) ->
   lager:error("Promotion of module ~p failed because ~p in ~p.", 
	       [ModName, Error, Pid]),
   {noreply, State#state{ pip = false }};

handle_info(Info, State) ->
   lager:warning("Unknown info ~p", [Info]),
   {noreply, State}.

%% @private
terminate(_Reason, #state{ modname = M }) ->
   fling_mochiglobal:purge(M),
   ok.

%% @private
code_change(_Old, State, _Extra) ->
   {ok, State}.

%% internal functions
schedule_tick(S) ->
   erlang:send_after(S, self(), ?TICK).

get_env(Setting, Default) ->
   case application:get_env(fling, Setting) of
      undefined -> Default;
      {ok, Value} -> Value
   end.

promote(Tid, ModName, GetKey, GetValue) ->
   {Pid, _Mref} = spawn_monitor(fun() -> create_mochiglobal(Tid, ModName, GetKey, GetValue) end),
   lager:notice("Promoting ETS table ~p to mochiglobal module ~p in pid ~p",
		[Tid, ModName, Pid]),
   ok.

create_mochiglobal(Tid, ModName, GetKey, GetValue) ->
   fling_mochiglobal:purge(ModName),
   ok = fling_mochiglobal:create(ModName, ets:tab2list(Tid), GetKey, GetValue).
