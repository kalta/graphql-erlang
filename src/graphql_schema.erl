-module(graphql_schema).
-behaviour(gen_server).

-include_lib("stdlib/include/qlc.hrl").
-include("graphql_schema.hrl").
-include("graphql_internal.hrl").

-export([start_link/0, reset/1]).
-export([
         all/1,
         insert/2, insert/3,
         load/2,
         get/2,
         lookup/2,
         lookup_enum_type/2,
         lookup_interface_implementors/1
        ]).
-export([resolve_root_type/2]).

-export([id/1]).

%% Callbacks
-export([init/1, handle_call/3, handle_cast/2, terminate/2, handle_info/2,
    code_change/3]).

-define(ENUMS, graphql_schema_enums).
-define(OBJECTS, graphql_schema_objects).

-record(state, {}).

-type namespace() :: term().


%% -- API ----------------------------
-spec start_link() -> any().
start_link() ->
    Res = gen_server:start_link({local, ?MODULE}, ?MODULE, [], []),
    reset(?DEFAULT_NAMESPACE),
    Res.

-spec reset(graphql:namespace()) -> ok.
reset(Namespace) ->
    ok = gen_server:call(?MODULE, {reset, Namespace}),
    ok = graphql_introspection:inject(Namespace),
    ok = graphql_builtins:standard_types_inject(Namespace),
    ok.

-spec insert(graphql:namespace(), any()) -> true.
insert(Namespace, S) -> insert(Namespace, S, #{ canonicalize => true }).

-spec insert(namespace(), any(), any()) -> true | false.
insert(Namespace, S, #{ canonicalize := true }) ->
    try graphql_schema_canonicalize:x(S) of
        Rec ->
            case gen_server:call(?MODULE, {insert, Namespace, Rec}) of
                true -> ok;
                false ->
                    Identify = fun({_, #{ id := ID }}) -> ID end,
                    {error, already_exists, Identify(S)}
            end
    catch
        Class:Reason ->
            error_logger:error_msg(
              "Schema canonicalization error: ~p stacktrace: ~p~n",
              [{Class,Reason}, erlang:get_stacktrace()]),
            {error, {schema_canonicalize, {Class, Reason}}}
    end;
insert(Namespace, S, #{}) ->
    gen_server:call(?MODULE, {insert, Namespace, S}).


-spec load(graphql:namespace(), any()) -> ok | {error, Reason}
  when Reason :: term().
load(Namespace, S) ->
    try graphql_schema_canonicalize:x(S) of
        #root_schema { query = Q } = Rec ->
            ok = graphql_introspection:augment_root(Q),
            insert_new_(Namespace, Rec);
        Rec ->
            insert_new_(Namespace, Rec)
    catch
        Class:Reason ->
            {error, {schema_canonicalize, {Class, Reason}}}
    end.

insert_new_(Namespace, Rec) ->
    case gen_server:call(?MODULE, {insert_new, Namespace, Rec}) of
        true -> ok;
        false -> {error, already_exists, id(Rec)}
    end.

-spec all(graphql:namespace()) -> [any()].
all(Namespace) ->
    [S || {N, S} <- ets:tab2list(?OBJECTS), N==Namespace].
    %ets:match_object(?OBJECTS, '_').

-spec get(graphql:namespace(), binary() | 'ROOT') -> schema_object().
get(Namespace, ID) ->
    case ets:lookup(?OBJECTS, {Namespace, ID}) of
       [{_, S}] -> S;
       _ -> exit(schema_not_found)
    end.

-spec lookup_enum_type(graphql:namespace(), binary()) -> binary() | not_found.
lookup_enum_type(Namespace, EnumValue) ->
    try ets:lookup_element(?ENUMS, {Namespace, EnumValue}, 3) of
        Ty -> ?MODULE:get(Namespace, Ty)
    catch
        error:badarg ->
            not_found
    end.

%% Find the implementors of a given interface. If this proves to be
%% too slow in practice, one can build an index in the schema over these
%% and use an index lookup instead. It should be fairly simple to do.
%%
%% However, in the spirit of getting something up and running, we start
%% with QLC in order to make a working system.
-spec lookup_interface_implementors(binary()) -> [binary()].
lookup_interface_implementors(IFaceID) ->
    QH = qlc:q([Obj#object_type.id
                || {_, Obj} <- ets:table(?OBJECTS),
                   element(1, Obj) == object_type,
                   lists:member(IFaceID, Obj#object_type.interfaces)]),
    qlc:e(QH).

-spec lookup(namespace(), binary() | 'ROOT') -> schema_object() | not_found.
lookup(Namespace, ID) ->
    case ets:lookup(?OBJECTS, {Namespace, ID}) of
       [{_, S}] -> S;
       _ -> not_found
    end.

-spec resolve_root_type(undefined | operation_type(), root_schema()) -> undefined | binary().
resolve_root_type(undefined, #root_schema { query = Q }) -> Q;
resolve_root_type({query, _}, #root_schema { query = Q }) -> Q;
resolve_root_type({mutation, _}, #root_schema { mutation = M }) -> M;
resolve_root_type({subscription, _}, #root_schema { subscription = S }) -> S.

id(#root_schema{}) -> 'ROOT';
id(#scalar_type{ id = ID }) -> ID;
id(#object_type{ id = ID}) -> ID;
id(#enum_type{ id = ID}) -> ID;
id(#interface_type{ id = ID}) -> ID;
id(#union_type{ id = ID}) -> ID;
id(#input_object_type{ id = ID }) -> ID.

%% -- CALLBACKS

-spec init([]) -> {ok, #state{}}.
init([]) ->
    _Tab1 = ets:new(?ENUMS,
         [named_table, protected, {read_concurrency, true}, set,
           {keypos, 1}]),
    _Tab = ets:new(?OBJECTS,
        [named_table, protected, {read_concurrency, true}, set,
         {keypos, 1}]),
         %{keypos, #object_type.id}]),
    {ok, #state{}}.

-spec handle_cast(any(), S) -> {noreply, S}
  when S :: #state{}.
handle_cast(_Msg, State) -> {noreply, State}.

-spec handle_call(M, any(), S) -> {reply, term(), S}
  when
    S :: #state{},
    M :: term().
handle_call({insert, Namespace, X}, _From, State) ->
    case determine_table(Namespace, X) of
        {error, unknown} ->
            {reply, {error, {schema, X}}, State};
        {enum, Tab, Enum, Id} ->
            ets:insert(Tab, {Id, X}),
            insert_enum(Namespace, Enum, X),
            {reply, true, State};
        {obj, Tab, Id} ->
            {reply, ets:insert(Tab, {Id, X}), State}
    end;
handle_call({insert_new, Namespace, X}, _From, State) ->
    case determine_table(Namespace, X) of
        {error, unknown} ->
            {reply, {error, {schema, X}}, State};
        {enum, Tab, Enum, Id} ->
            case ets:insert_new(Tab, {Id, X}) of
                false ->
                   {reply, false, State};
                true ->
                   insert_enum(Namespace, Enum, X),
                   {reply, true, State}
            end;
        {obj, Tab, Id} ->
            {reply, ets:insert_new(Tab, {Id, X}), State}
    end;
handle_call({reset, Namespace}, _From, State) ->
    lists:foreach(
        fun(Tuple) ->
            case element(1, Tuple) of
                {N, _} =Id when N==Namespace ->
                    ets:delete_object(?OBJECTS, Id);
                _ ->
                    ok
            end
        end,
        all(Namespace)),
    true = ets:delete_all_objects(?OBJECTS),
    {reply, ok, State};
handle_call(_Msg, _From, State) ->
    {reply, {error, unknown_call}, State}.

-spec handle_info(term(), S) -> {noreply, S}
  when S :: #state{}.
handle_info(_Msg, State) -> {noreply, State}.

-spec terminate(any(), any()) -> any().
terminate(_, _) -> ok.

-spec code_change(term(), S, term()) -> {ok, S}
  when S :: #state{}.
code_change(_OldVsn, State, _Aux) -> {ok, State}.

%% -- INTERNAL FUNCTIONS -------------------------

%% determine_table/1 figures out the table and key an object belongs to
determine_table(Namespace, #root_schema{id=Id}) -> {obj, ?OBJECTS, {Namespace, Id}};
determine_table(Namespace, #object_type{id=Id}) -> {obj, ?OBJECTS, {Namespace, Id}};
determine_table(Namespace, #enum_type{id=Id}) -> {enum, ?OBJECTS, ?ENUMS, {Namespace, Id}};
determine_table(Namespace, #interface_type{id=Id}) -> {obj, ?OBJECTS, {Namespace, Id}};
determine_table(Namespace, #scalar_type{id=Id}) -> {obj, ?OBJECTS, {Namespace, Id}};
determine_table(Namespace, #input_object_type{id=Id}) -> {obj, ?OBJECTS, {Namespace, Id}};
determine_table(Namespace, #union_type{id=Id}) -> {obj, ?OBJECTS, {Namespace, Id}};
determine_table(_Namespace, _) -> {error, unknown}.

%% insert enum values
insert_enum(Tab, Namespace, #enum_type { id = ID, values = VMap }) ->
    Vals = maps:to_list(VMap),
    [begin
        ets:insert(Tab, {{Namespace, Key}, Value, {Namespace, ID}})
      end || {Value, #enum_value { val = Key }} <- Vals],
    ok.
