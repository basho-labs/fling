%% @doc Abuse module constant pools as a "read-only shared heap" (since erts 5.6) for non-binary
%%      Erlang terms.
%%      <a href="http://www.erlang.org/pipermail/erlang-questions/2009-March/042503.html">[1]</a>.
%%      Based on <a href="https://mochiweb.googlecode.com/svn/trunk/src/mochiglobal.erl">[2]</a>.
%%
%%      <B>Note:</B> We are explicitly using tuples here because we expect to
%%      use this to speed up ETS lookups and ETS stores tuples.
%%
%%      This version stores keys and values in a large opaque data object - currently
%%      implemented as a `gb_tree' for lookups. This is mostly an experiment to compare
%%      performance improvements (if any) vs. our current implementation.

-module(fling_gb).
-export([create/3, 
	 create/4, 
	 get/2, 
	 get/3,
	 mode/2,
	 to_list/1,
	 gen_module_name/0,
	 purge/1]).

-define(ALL, all).
-define(GETTER, term).

-type get_expr_fun() :: fun((tuple()) -> any()).

-spec create(        L :: [ tuple() ], 
	        GetKey :: get_expr_fun(),
	      GetValue :: get_expr_fun() ) -> ModName :: atom().
%% @doc create a module using the list of tuples given. The functions
%% passed in should return the key and the value respectively when
%% provided an element from the list of input tuples. Each function will be given 
%% the same element of L as input.
%%
%% A simple example might be:
%% <pre>
%% GetKey = fun({K, _V}) -> K end.
%% GetValue = fun({_K, V}) -> V end.
%% </pre>
%%
%% A more complex example might be:
%% <pre>
%% -record(person, { name, phone }).
%% % use phone as the key for this record lookup
%% get_key(#person{ phone = Phone }) -> Phone.
%% % use entire record tuple as the value
%% get_value(E) -> E.
%% </pre>
create(L, GetKey, GetValue) when is_list(L) andalso L /= [] 
				 andalso is_function(GetKey) andalso is_function(GetValue)->
    ModName = gen_module_name(),
    ok = create(ModName, L, GetKey, GetValue),
    ModName.

-spec create(  ModName :: atom(), 
	             L :: [ tuple() ],  
	        GetKey :: get_expr_fun(),
	      GetValue :: get_expr_fun() 
	    ) -> ok | {error, Reason :: term()}.
%% @doc create and load a module using the given module name, and constructed
%% using the list of tuples given.
%% {@link create/3}
create(ModName, L, GetKey, GetValue) when is_atom(ModName) 
					  andalso is_list(L) andalso L /= [] 
					  andalso is_function(GetKey) andalso is_function(GetValue) ->
    Bin = compile(ModName, L, GetKey, GetValue),
    code:purge(ModName),
    case code:load_binary(ModName, atom_to_list(ModName) ++ ".erl", Bin) of
	    {module, ModName} -> ok;
	    Error -> Error
    end.

-spec get(ModName :: atom(), Key :: term()) -> any() | undefined.
%% @equiv get(ModName, K, undefined)
get(ModName, K) ->
    get(ModName, K, undefined).

-spec get(ModName :: atom(), Key :: term(), Default :: term()) -> term().
%% @doc Get the term for K or return Default if K is not found.
get(ModName, K, Default) ->
    Data = ModName:?GETTER(),
    case gb_trees:lookup(K, Data) of
       none -> Default;
       {value, V} -> V
    end.

-spec mode(ModName :: atom(), Tid :: ets:tid()) -> {ets, Tid :: ets:tid()}
						   | {mg, ModName :: atom()}.
%% @doc Get the current mode.
mode(ModName, Tid) ->
   try
      ModName:mode()
   catch
      error:undef ->
	 {ets, Tid}
   end.

-spec to_list(ModName :: atom()) -> [ tuple() ].
%% @doc Return all input tuples from the constructed module as a list.
to_list(ModName) when is_atom(ModName) ->
   ModName:all().

-spec purge( ModName :: atom() ) -> boolean().
%% @doc Purges and removes the given module
purge(ModName) when is_atom(ModName) ->
    code:purge(ModName),
    code:delete(ModName).

-spec gen_module_name() -> atom().
%% @doc Generate a unique random module name
gen_module_name() ->
    list_to_atom("fling$gb$" ++ md5hex(term_to_binary(erlang:make_ref()))).

%% internal functions
% @private
-spec md5hex( binary() ) -> string().
md5hex(Data) ->
    binary_to_list(hexlify(erlang:md5(Data))).

%% http://stackoverflow.com/a/29819282
-spec hexlify( binary() ) -> binary().
hexlify(Bin) when is_binary(Bin) ->
    << <<(hex(H)),(hex(L))>> || <<H:4,L:4>> <= Bin >>.

hex(C) when C < 10 -> $0 + C;
hex(C) -> $a + C - 10.

-spec compile(  ModName :: atom(), 
	              L :: [ tuple() ], 
	         GetKey :: get_expr_fun(),
	       GetValue :: get_expr_fun() ) -> binary().
compile(Module, L, GetKey, GetValue) ->
    Data = build_opaque_data(L, GetKey, GetValue),
    {ok, Module, Bin} = compile:forms(forms(Module, L, Data),
                                      [verbose, report_errors]),
    Bin.

-spec build_opaque_data(        L :: [tuple()],
			   GetKey :: get_expr_fun(),
			 GetValue :: get_expr_fun() ) -> term().
build_opaque_data(L, GetKey, GetValue) ->
    lists:foldl(fun(E, Acc) -> gb_trees:enter(GetKey(E), GetValue(E), Acc) end, gb_trees:empty(), L).

-spec forms(  ModName :: atom(), 
	            L :: [ tuple() ], 
	         Data :: term() ) -> [erl_syntax:syntaxTree()].
forms(Module, L, Data) ->
    [erl_syntax:revert(X) || X <- [ module_header(Module), 
				    handle_exports(?GETTER), 
				    make_all(L),
				    make_mode(Module),
				    make_data_term(?GETTER, Data) ] ].

-spec module_header( ModName :: atom() ) -> erl_syntax:syntaxTree().
%% -module(Module).
module_header(Module) ->
   erl_syntax:attribute(
     erl_syntax:atom(module),
     [erl_syntax:atom(Module)]).

%% -export([ term/1, all/0 ]).
-spec handle_exports( Getter :: atom() ) -> erl_syntax:syntaxTree().
handle_exports(Getter) ->
    erl_syntax:attribute(
       erl_syntax:atom(export),
       [erl_syntax:list(
         [erl_syntax:arity_qualifier(
            erl_syntax:atom(Getter),
            erl_syntax:integer(0)),
	  erl_syntax:arity_qualifier(
	    erl_syntax:atom(mode),
	    erl_syntax:integer(0)),
	  erl_syntax:arity_qualifier(
	    erl_syntax:atom(?ALL),
	    erl_syntax:integer(0))
	 ])]).

%% all() -> L.
-spec make_all([ tuple() ]) -> erl_syntax:syntaxTree().
make_all(L) ->
    erl_syntax:function(
      erl_syntax:atom(?ALL),
      [erl_syntax:clause([], none, [erl_syntax:abstract(L)])]).

make_mode(ModName) ->
    erl_syntax:function(
      erl_syntax:atom(mode),
      [erl_syntax:clause([], none, [erl_syntax:abstract({mg, ModName})])]).

%% term(K) -> V;
-spec make_data_term(   Getter :: atom(), 
			  Data :: term() ) -> [erl_syntax:syntaxTree()].
make_data_term(Getter, Data) ->
    erl_syntax:function(
       erl_syntax:atom(Getter),
       [erl_syntax:clause([], none, [erl_syntax:abstract(Data)])]).
	
%%
%% Tests
%%
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
basic_test() -> 
    L = [{a,1}, {b,2}, {c,3}],
    GetKey = fun({K, _V}) -> K end,
    GetValue = fun({_K, V}) -> V end,
    Mod = create(L, GetKey, GetValue),
    ?assertEqual(L, to_list(Mod)),
    ?assertEqual(1, ?MODULE:get(Mod, a)),
    ?assertEqual(2, ?MODULE:get(Mod, b)),
    ?assertEqual(3, ?MODULE:get(Mod, c)),
    ?assertEqual(undefined, ?MODULE:get(Mod, d)).

-record(person, {name, phone}).
record_test() ->
   L = [ #person{name="mike", phone=1}, #person{name="joe", phone=2}, #person{name="robert", phone=3} ],
   GetKey = fun(#person{ phone = P }) -> P end,
   GetValue = fun(#person{ name = N }) -> N end,
   Mod = create(L, GetKey, GetValue),
   ?assertEqual(L, to_list(Mod)),
   ?assertEqual("mike", ?MODULE:get(Mod, 1)),
   ?assertEqual("joe", ?MODULE:get(Mod, 2)),
   ?assertEqual("robert", ?MODULE:get(Mod, 3)).

-endif.
