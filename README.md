fling
=====
Fling is an extraction of an idea in riak_core. The basic idea is a cache manager
handles writes and reads to ETS for a period of time, and after the writes are
over for a period of time, the manager "promotes" the ETS data into a dynamically
constructed and loaded Erlang module.

Rationale
---------
ETS is pretty fast already. Yes, it is. ETS also can have a long tail where
get requests can take hundreds or thousands of microseconds instead of the
typical 1-3 microseconds.

What we see by abusing the constant pools in Erlang modules is we can chop
that latency tail significantly, down from a ceiling of mid-thousands of 
microseconds to (low) hundreds of microseconds. (Full benchmarks are
in progress and forthcoming.)

Concept
-------
The general idea is the caller creates and sets up an ETS table however she
wants it to be.  The table can be used as normal for inserts, lookups, etc.

At some point, the caller uses `fling:manage/4` and fling begins to manage
the ETS cache.  After a number of ticks (default 5) of X seconds (default 5)
fling builds, compiles and loads a module version of the data in ETS.

At that point, the caller can use the dynamically generated module to
lookup values.

If a new write comes in, fling accepts the write on the ETS layer and 
purges the generated module - and begins a countdown to a new generated
module.

Drawbacks
---------
Well, first, this pattern **does not** work well for use cases where writes
are frequent.  It works best in situations where there is zero to very low
write frequency and very high read concurrency.

It keeps two copies of your data around once the module has been loaded. 
One copy sits around in ETS, unused and unloved for the most part.

Second, the generated module(s) use entries in Erlang's finite atom table.
It *is* possible to exhaust the atom table by creating too many fling
modules. In general, each additional module takes 1 atom. The terms
used in the generated module are the atoms `term` and `all` - so after
the first module, the only additional atom is the name of the module 
itself.

Third, compiling a module takes a long time. For a module with 10000
entries, it takes about 5 wall-clock seconds to construct the abstract
syntax tree, compile it into byte code and load it into the run time
system.

Example
-------
```erlang
1> Tid = ets:new(example, []).
2> ModName = fling:gen_module_name().
3> GetKey = fun({K, _V}) -> K end.
4> GetValue = fun({_K, V}) -> V end.
5> Pid = fling:manage(Tid, GetKey, GetValue, ModName).
6> fling:put(Pid, [{a,1},{b,2},{c,3}]).
7> Mode = fling:mode(Pid).
8> fling:get(Mode, Tid, ModName, a).
9> timer:sleep(60*1000).
10> Mode0 = fling:mode(Pid).
11> fling:get(Mode0, Tid, ModName, a).
12> fling:get(Mode, Tid, ModName, a) == fling:get(Mode0, Tid, ModName, a).
```

