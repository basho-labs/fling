-module(fling_smoke).
-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").

smoke_test_() ->
    {timeout, 60, fun() -> smoke() end}.

smoke_get_key({K, _V}) -> K.
smoke_get_value({_K, V}) -> V.

smoke() ->
    application:load(fling),
    application:set_env(fling, secs_per_tick, 1),
    {ok, _} = application:ensure_all_started(fling),

    Tid = ets:new(fling_smoke, []),
    Data = [ {X, list_to_binary([65+(X rem 26)])} || X <- lists:seq(1,1000) ],
    true = ets:insert(Tid, Data),
    ModName = fling:gen_module_name(),
    Pid = fling:manage(Tid, fun smoke_get_key/1, fun smoke_get_value/1, ModName),
    ModeA = fling:mode(Tid, ModName),
    ?assertEqual({ets, Tid}, ModeA),
    ?assertEqual(fling:mode_sync(Pid), fling:mode(Tid, ModName)),
    ?assertEqual([{1, <<"B">>}], fling:get(ModeA, 1)),
    ?assertEqual([{26, <<"A">>}], fling:get(ModeA, 26)),
    ?assertEqual(undefined, fling:get(ModeA, 999999)),
    {ok, Waits} = wait_until(20, fun() -> is_mode_mg(fling:mode(Tid, ModName)) end),
    ?debugFmt("waited ~p secs", [20-Waits]),
    ModeB = fling:mode(Tid, ModName),
    ?assertEqual({mg, ModName}, ModeB),
    ?assertEqual(fling:mode_sync(Pid), fling:mode(Tid, ModName)),
    ?assertEqual([{1, <<"B">>}], fling:get(ModeA, 1)),
    ?assertEqual([{26, <<"A">>}], fling:get(ModeA, 26)),
    ?assertEqual(<<"B">>, fling:get(ModeB, 1)),
    ?assertEqual(<<"A">>, fling:get(ModeB, 26)),
    ?assertEqual(undefined, fling:get(ModeB, 999999)).

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
