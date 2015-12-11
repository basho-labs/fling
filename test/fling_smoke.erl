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
    ?assertEqual(ets, fling:mode(Pid)),
    ?assertEqual([{1, <<"B">>}], fling:get(ets, Tid, ModName, 1)),
    ?assertEqual([{26, <<"A">>}], fling:get(ets, Tid, ModName, 26)),
    ?assertEqual(undefined, fling:get(ets, Tid, ModName, 999999)),
    {ok, Waits} = wait_until(20, fun() -> case fling:mode(Pid) of ets -> false; mg -> true end end),
    ?debugFmt("waited ~p secs", [Waits]),
    ?assertEqual(mg, fling:mode(Pid)),
    ?assertEqual([{1, <<"B">>}], fling:get(ets, Tid, ModName, 1)),
    ?assertEqual([{26, <<"A">>}], fling:get(ets, Tid, ModName, 26)),
    ?assertEqual(<<"B">>, fling:get(mg, Tid, ModName, 1)),
    ?assertEqual(<<"A">>, fling:get(mg, Tid, ModName, 26)),
    ?assertEqual(undefined, fling:get(mg, Tid, ModName, 999999)).

wait_until(0, _F) -> timeout;
wait_until(Limit, F) ->
    case F() of
        true -> {ok, Limit};
        false -> 
            timer:sleep(1000),
            wait_until(Limit - 1, F)
    end.
