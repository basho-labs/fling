%% @doc This test checks for a race condition where a new 
%% put reverts the cache manager's gen_server into ets mode
%% but there's a lag between when the put is processed and when
%% gets into the module stop working.
%%
%% This test measures how long the lag is.
-module(fling_promote_writes).
-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").

promote_test_() ->
    {timeout, 60, fun() -> promote() end}.

promote() ->
    application:load(fling),
    application:set_env(fling, secs_per_tick, 1),
    {ok, _} = application:ensure_all_started(fling),

    Tid = ets:new(fling_smoke, []),
    Data = [ {X, list_to_binary([65+(X rem 26)])} || X <- lists:seq(1,1000) ],
    true = ets:insert(Tid, Data),
    ModName = fling:gen_module_name(),
    Pid = fling:manage(Tid, fun({K, _V}) -> K end, fun({_K, V}) -> V end, ModName),

    {ok, Waits} = wait_until(20, fun() -> is_mode_mg(fling:mode(Tid, ModName)) end),
    ?debugFmt("waited ~p secs for promotion", [20-Waits]),
    Mode = fling:mode(Tid, ModName),
    ?assertEqual(<<"B">>, fling:get(Mode, 1)),
    %% observe there is nothing up my sleeve...
    ?assertEqual(undefined, fling:get(Mode, 1001)),
    fling:put(Pid, {1001, <<"N">>}),
    {ok, W} = wait_until(20, fun() -> fling:mode_sync(Pid) == fling:mode(Tid, ModName) end),
    ?debugFmt("waited ~p secs for mode to synchronize", [20-W]),
    ?assertEqual([{1001, <<"N">>}], fling:get(fling:mode(Tid, ModName), 1001)),

    {ok, W2} = wait_until(20, fun() -> is_mode_mg(fling:mode(Tid, ModName)) end),
    ?debugFmt("waited ~p secs for another promotion", [20-W2]),
    ?assertEqual(<<"N">>, fling:get(fling:mode(Tid, ModName), 1001)).

wait_until(0, _F) -> timeout;
wait_until(Limit, F) ->
    case F() of
        true -> {ok, Limit};
        false -> 
            timer:sleep(1000),
            wait_until(Limit - 1, F)
    end.

is_mode_mg({ets, _}) -> false;
is_mode_mg({mg, _}) -> true.
