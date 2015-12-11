-module(fling_basic).
-compile([export_all]).

%% basic benchmarking test

-include_lib("eunit/include/eunit.hrl").

-record(person, {id, name}).

bench_ets_test_() ->
    {timeout, 60, fun() -> basic_ets_bench() end}.

bench_mg_test_() ->
    {timeout, 60, fun() -> basic_mg_bench() end}.

names() ->
    [<<"sally">>, <<"robert">>, <<"mike">>, <<"joe">>, <<"fred">>, <<"doug">>, 
     <<"jennifer">>, <<"enrique">>, <<"hodor">>, <<"alex">>, <<"bobbi">>, 
     <<"gordon">>, <<"andrew">>, <<"john">>, <<"jon">>, <<"frida">>].

choose(I, L) ->
    lists:nth(((I rem length(L))+1), L).

basic_ets_bench() ->
    random:seed({1,2,3}), %% deterministic for now
    Tid = make_ets([{read_concurrency, false}, {write_concurrency, false}]),
    CacheSize = 10000,
    Data = build_data_set(fun make_record/1, CacheSize),
    Out = ets_bench(Tid, Data, CacheSize, CacheSize * 10),
    lists:foreach(fun({_Timing, {Id, Name}}) -> ?assertEqual(Name, choose(Id, names())) end, Out),
    Out1 = lists:foldl(fun({Timing, _Value}, Acc) -> orddict:update_counter(Timing, 1, Acc) end, 
                       orddict:new(), Out),
    ?debugFmt("ETS get microseconds: ~p~n", [orddict:to_list(Out1)]).

basic_mg_bench() ->
    random:seed({1,2,3}), %% deterministic for now
    CacheSize = 10000,
    Data = build_data_set(fun make_record/1, CacheSize),
    ModName = fling:gen_module_name(),
    {Time, ok} = promote(ModName, Data),
    ?debugFmt("~p microseconds to compile ~p with ~p elements~n", [Time, ModName, CacheSize]),
    %% this is a random uniform distribute of keys
    Out = mg_bench(ModName, CacheSize, CacheSize * 10),
    lists:foreach(fun({_Timing, {Id, Name}}) -> ?assertEqual(Name, choose(Id, names())) end, Out),
    Out1 = lists:foldl(fun({Timing, _Value}, Acc) -> orddict:update_counter(Timing, 1, Acc) end, 
                       orddict:new(), Out),
    ?debugFmt("mg get microseconds: ~p~n", [orddict:to_list(Out1)]),

    %% this is the 1000 worst case lookups
    Out2 = mg_bench_worst_case(ModName, 1000),
    Out3 = lists:foldl(fun({Timing, _Value}, Acc) -> orddict:update_counter(Timing, 1, Acc) end, 
                       orddict:new(), Out2),
    ?debugFmt("mg get 1000 worst case keys microseconds: ~p~n", [orddict:to_list(Out3)]).

promote(ModName, Data) -> 
    timer:tc(fun() -> 
    fling_mochiglobal:create(ModName, Data, fun basic_get_key/1, fun basic_get_value/1) 
                          end).

mg_bench(Mod, CacheSize, GetOps) ->
    [ begin
        X = random:uniform(CacheSize),
        timer:tc(fun() -> mg_get_op(Mod, X) end) 
      end || _ <- lists:seq(1, GetOps) ].

mg_bench_worst_case(Mod, EndKey) ->
    %% this will fetch the `EndKey' "worst case" keys. These are the keys in
    %% the compiled module which are farthest from where the Erlang run-time
    %% system starts to evaluate function heads.  In the basic benchmark we
    %% generate 10000 keys and the keys are stored in reverse order, which
    %% means that id 10000 is the very 1st key in the list of function heads
    %% and id 1 is the very last key.
    
    [ timer:tc(fun() -> mg_get_op(Mod, X) end) || X <- lists:seq(EndKey, 1, -1) ].

make_ets(Options) ->
    ets:new(fling_bench_test, [{keypos, 2}] ++ Options).

build_data_set(Create, CacheSize) when is_function(Create) ->
    [ Create(X) || X <- lists:seq(1, CacheSize) ].

ets_bench(Tid, Data, CacheSize, GetOps) ->
    ets:insert(Tid, Data),

    [ begin
        X = random:uniform(CacheSize),
        timer:tc(fun() -> ets_get_op(Tid, X) end) 
      end || _ <- lists:seq(1, GetOps) ].

make_record(Id) ->
    #person{ id = Id, name = choose(Id, names()) }.
    
ets_get_op(Tid, Id) ->
    case ets:lookup(Tid, Id) of
        [] -> {Id, undefined};
        [#person{ name = N }] -> {Id, N}
    end.

basic_get_key(#person{ id = Id }) -> Id.
basic_get_value(#person{ id = Id, name = N}) -> {Id, N}.

mg_get_op(Mod, Id) ->
    fling_mochiglobal:get(Mod, Id).
