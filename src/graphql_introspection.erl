-module(graphql_introspection).

-include("graphql_schema.hrl").
-include_lib("graphql/include/graphql.hrl").

-export([inject/0, augment_root/2]).

-export([execute/4]).

%% Resolvers for the introspection
-export([
    schema_resolver/3,
    type_resolver/3,
    schema_types/3,
    query_type/3,
    mutation_type/3,
    subscription_type/3
]).

-spec augment_root(graphql:namespace(), QueryObj) -> ok
  when QueryObj :: binary().

augment_root(Namespace, QName) ->
    #object_type{ fields = Fields } = Obj = graphql_schema:get(Namespace, QName),
    Schema = #schema_field {
                ty = {non_null, <<"__Schema">>},
                description = <<"The introspection schema">>,
                resolve = fun ?MODULE:schema_resolver/3
               },
    Type = #schema_field {
              ty = <<"__Type">>,
              description = <<"The introspection type">>,
              args = #{
                <<"name">> =>
                    #schema_arg { ty = {non_null, <<"String">>},
                                  description = <<"The type to query">> }
               },
              resolve = fun ?MODULE:type_resolver/3 },
    Augmented = Obj#object_type { fields = Fields#{
                                             <<"__schema">> => Schema,
                                             <<"__type">> => Type
                                            }},
    true = graphql_schema:insert(Namespace, Augmented, #{}),
    ok.

schema_resolver(Ctx, none, #{}) ->
    #{namespace:=Namespace} = Ctx,
    {ok, #{ <<"directives">> =>
                [directive(Namespace, include),
                 directive(Namespace, skip)]}}.

type_resolver(Ctx, none, #{ <<"name">> := N }) ->
    #{namespace:=Namespace} = Ctx,
    case graphql_schema:lookup(Namespace, N) of
        not_found -> {ok, null};
        Ty -> render_type(Namespace, Ty)
    end.

query_type(Ctx, _Obj, _) ->
    #{namespace:=Namespace} = Ctx,
    #root_schema{ query = QType } = graphql_schema:get(Namespace, 'ROOT'),
    render_type(Namespace, QType).

mutation_type(Ctx, _Obj, _) ->
    #{namespace:=Namespace} = Ctx,
    #root_schema { mutation = MType } = graphql_schema:get(Namespace, 'ROOT'),
    case MType of
        undefined -> {ok, null};
        MT -> render_type(Namespace, MT)
    end.

subscription_type(Ctx, _Obj, _) ->
    #{namespace:=Namespace} = Ctx,
    #root_schema { subscription = SType } = graphql_schema:get(Namespace, 'ROOT'),
    case SType of
        undefined -> {ok, null};
        ST -> render_type(Namespace, ST)
    end.

schema_types(Ctx, _Obj, _Args) ->
    Pass = fun
        (#root_schema{}) -> false;
        (_) -> true
    end,
    #{namespace:=Namespace} = Ctx,
    Types = [X || X <- graphql_schema:all(Namespace), Pass(X)],
    {ok, [render_type(Namespace, Ty) || Ty <- Types]}.

%% Main renderer. Calls out to the subsets needed
render_type(Namespace, Name) when is_binary(Name) ->
    case graphql_schema:lookup(Namespace, Name) of
        not_found ->
           throw({not_found, Name});
       Ty -> render_type(Namespace, Ty)
    end;
render_type(Namespace, Ty) -> {ok, #{
    <<"kind">> => type_kind(Ty),
    <<"name">> => type_name(Ty),
    <<"description">> => type_description(Ty),
    <<"fields">> => type_fields(Namespace, Ty),
    <<"interfaces">> => type_interfaces(Namespace, Ty),
    <<"possibleTypes">> => type_possibilities(Namespace, Ty),
    <<"enumValues">> => type_enum_values(Ty),
    <<"inputFields">> => type_input_fields(Namespace, Ty),
    <<"ofType">> => type_unwrap(Namespace, Ty) }}.

type_kind(#scalar_type{}) -> <<"SCALAR">>;
type_kind(#object_type {}) -> <<"OBJECT">>;
type_kind(#input_object_type {}) -> <<"INPUT_OBJECT">>;
type_kind(#interface_type{}) -> <<"INTERFACE">>;
type_kind(#union_type{}) -> <<"UNION">>;
type_kind(#enum_type{}) -> <<"ENUM">>;
type_kind({list, _}) -> <<"LIST">>;
type_kind({non_null, _Ty}) -> <<"NON_NULL">>.

type_name(#scalar_type { id = N }) -> N;
type_name(#enum_type { id = N }) -> N;
type_name(#interface_type { id = N }) -> N;
type_name(#object_type { id = N }) -> N;
type_name(#input_object_type { id = N }) -> N;
type_name(#union_type { id = N }) -> N;
type_name({non_null, _}) -> null;
type_name({list, _}) -> null.

type_description(#object_type { description = D }) -> D;
type_description(#input_object_type { description = D }) -> D;
type_description(#interface_type { description = D}) -> D;
type_description(#enum_type{ description = D}) -> D;
type_description(#scalar_type{ description = D}) -> D;
type_description(_) -> null.

type_interfaces(Namespace, #object_type{ interfaces = IFs }) ->
    ?LAZY({ok, [render_type(Namespace, Ty) || Ty <- IFs]});
type_interfaces(_Namespace, _) -> null.

type_possibilities(Namespace, #interface_type { id = ID }) ->
    ?LAZY(interface_implementors(Namespace, ID));
type_possibilities(Namespace, #union_type { types = Types }) ->
    ?LAZY({ok, [render_type(Namespace, Ty) || Ty <- Types]});
type_possibilities(_Namespace, _) -> null.

type_enum_values(#enum_type { values = VMap }) ->
    [begin
         {ok, R} = render_enum_value(V),
         R
     end || V <- maps:to_list(VMap)];
type_enum_values(_) -> null.

type_unwrap(Namespace, {list, Ty}) -> {ok, U} = render_type(Namespace, Ty), U;
type_unwrap(Namespace, {non_null, Ty}) -> {ok, U} = render_type(Namespace, Ty), U;
type_unwrap(_Namespace, _) -> null.

type_input_fields(Namespace, #input_object_type{ fields = FS }) ->
    ?LAZY({ok, [render_input_value(Namespace, F) || F <- maps:to_list(FS)]});
type_input_fields(_Namespace, _) -> null.

type_fields(Namespace, #object_type { fields = FS }) ->
    ?LAZY({ok, [render_field(Namespace, F) || F <- maps:to_list(FS), interesting_field(F)]});
type_fields(Namespace, #interface_type { fields = FS }) ->
    ?LAZY({ok, [render_field(Namespace, F) || F <- maps:to_list(FS), interesting_field(F)]});
type_fields(_Namespace, _) -> null.

interesting_field({<<"__schema">>, #schema_field {}}) -> false;
interesting_field({<<"__type">>, #schema_field{}}) -> false;
interesting_field({_, _}) -> true.

render_field(Namespace, {Name, #schema_field {
                       description = Desc,
                       args = Args,
                       ty = Ty,
                       deprecation = Deprecation }
             }) ->
    {IsDeprecated, DeprecationReason} = render_deprecation(Deprecation),
    {ok, #{
        <<"name">> => Name,
        <<"description">> => Desc,
        <<"args">> => ?LAZY({ok, [render_input_value(Namespace, IV) || IV <- maps:to_list(Args)]}),
        <<"type">> => ?LAZY(render_type(Namespace, Ty)),
        <<"isDeprecated">> => IsDeprecated,
        <<"deprecationReason">> => DeprecationReason
      }}.

render_input_value(Namespace, {K, #schema_arg { ty = Ty, description = Desc }}) ->
    {ok, #{
        <<"name">> => K,
        <<"description">> => Desc,
        <<"type">> => ?LAZY(render_type(Namespace, Ty)),
        <<"defaultValue">> => null
    }}.

interface_implementors(Namespace, ID) ->
    Pass = fun
        (#object_type { interfaces = IFs }) -> lists:member(ID, IFs);
        (_) -> false
    end,
    {ok, [render_type(Namespace, Ty) || Ty <- graphql_schema:all(Namespace), Pass(Ty)]}.

render_enum_value({_Value, #enum_value{
                              val = Key,
                              description = Desc,
                              deprecation = Deprecation }
                  }) ->
    {IsDeprecated, DeprecationReason} = render_deprecation(Deprecation),
    {ok, #{
       <<"name">> => Key,
       <<"description">> => Desc,
       <<"isDeprecated">> => IsDeprecated,
       <<"deprecationReason">> => DeprecationReason
     }}.

render_deprecation(undefined) ->
    {false, null};
render_deprecation(Reason) when is_binary(Reason) ->
    {true, Reason}.

%% -- SCHEMA DEFINITION -------------------------------------------------------
-spec inject() -> ok.
inject() ->
    inject(?DEFAULT_NAMESPACE).

-spec inject(namespace()) -> ok.
inject(Namespace) ->
    Schema = {object, #{
                id => '__Schema',
                resolve_module => ?MODULE,
                description => "Schema Definitions in GraphQL",
                fields => #{
                  types => #{
                    type => {non_null, ['__Type!']},
                    description => "The types in the Schema that are defined",
                    resolve => fun ?MODULE:schema_types/3 },
                  queryType => #{
                    type => '__Type!',
                    description => "The type of the 'query' entries.",
                    resolve => fun ?MODULE:query_type/3 },
                  mutationType => #{
                    type => '__Type',
                    description => "The type of the 'mutation' entries.",
                    resolve => fun ?MODULE:mutation_type/3 },
                  subscriptionType => #{
                    type => '__Type',
                    description => "The type of the 'subscription' entries.",
                    resolve => fun ?MODULE:subscription_type/3 },
                  directives => #{
                    type => {non_null, ['__Directive!']},
                    description => "The directives the server currently understands"
                   }}}},
    Type = {object, #{
              id => '__Type',
              resolve_module => ?MODULE,
              description => "Descriptions of general types/objects in the model",
              fields => #{
                kind => #{
                  type => '__TypeKind!',
                  description => "The general kind of the type" },
                name => #{
                  type => 'String',
                  description => "The name of the type" },
                description => #{
                  type => 'String',
                  description => "The description field of the type" },
                %% OBJECT and INTERFACE only
                fields => #{
                  type => ['__Field!'],
                  description => "The fields in the object/interface",
                  args => #{
                    includeDeprecated => #{
                      type => 'Bool',
                      description => "Should deprecated fields be included or not",
                      default => false
                     }}},
                %% OBJECT only
                interfaces => #{
                  type => ['__Type!'],
                  description => "The interfaces the object adheres to"
                 },
                %% INTERFACE and UNION only
                possibleTypes => #{
                  type => ['__Type!'],
                  description => "The possible types of an interface and/or union"
                 },
                %% ENUM
                enumValues => #{
                  type => ['__EnumValue!'],
                  description => "The possible values of an Enum",
                  args => #{
                    includeDeprecated => #{
                      type => 'Bool',
                      description => "Should deprecated fields be included or not",
                      default => false
                     }}},
                %% INPUT OBJECT ONLY
                inputFields => #{
                  type => ['__InputValue!'],
                  description => "The possible inputfields of the Input Object" },
                %% NON NULL / LIST
                ofType => #{
                  type => '__Type',
                  description => "The underlying type of a non-null or list type" }
               }}},
    Field = {object, #{
               id => '__Field',
               resolve_module => ?MODULE,
               description => "Fields in a Schema",
               fields => #{
                 name => #{
                   type => 'String!',
                   description => "The name of the field" },
                 description => #{
                   type => 'String',
                   description => "The description of a field" },
                 args => #{
                   type => ['__InputValue!'],
                   description => "The possible arguments of a given field" },
                 type => #{
                   type => '__Type',
                   description => "The type of the given field" },
                 isDeprecated => #{
                   type => 'Bool!',
                   description => "True if the field is deprecated, false otherwise" },
                 deprecationReason => #{
                   type => 'String',
                   description => "The Reason for the deprecation, if any" }
                }}},
    InputValue = {object, #{
                    id => '__InputValue',
                    resolve_module => ?MODULE,
                    description => "InputValues in the Schema",
                    fields => #{
                      name => #{
                        type => 'String!',
                        description => "The name of the inputvalue" },
                      description => #{
                        type => 'String',
                        description => "The description of the field" },
                      type => #{
                        type => '__Type!',
                        description => "The type of the field" },
                      defaultValue => #{
                        type => 'String',
                        description => "The default value of the field rendered as a string" }
                     }}},
    Enum = {object, #{
              id => "__EnumValue",
              resolve_module => ?MODULE,
              description => "Introspection of enumeration values",
              fields => #{
                name => #{
                  type => 'String!',
                  description => "The name of the enumeration value" },
                description => #{
                  type => 'String',
                  description => "The description of the value" },
                isDeprecated => #{
                  type => 'Bool!',
                  description => "True if this is a deprecated field" },
                deprecationReason => #{
                  type => 'String',
                  description => "The reason for the deprecation, if any" }
               }}},
    TypeKind = {enum, #{
                  id => '__TypeKind',
                  description => "The different type schemes (kinds)",
                  resolve_module => graphql_enum_coerce,
                  values => #{
                    'SCALAR' => #{ value => 1, description => "Scalar types" },
                    'OBJECT' => #{ value => 2, description => "Object types" },
                    'INTERFACE' => #{ value => 3, description => "Interface types" },
                    'UNION' => #{ value => 4, description => "Union types" },
                    'ENUM' => #{ value => 5, description => "Enum types" },
                    'INPUT_OBJECT' => #{ value => 6, description => "InputObject types" },
                    'LIST' => #{ value => 7, description => "List types" },
                    'NON_NULL' => #{ value => 8, description => "NonNull types" }
                   }}},
    Directive = {object, #{
                   id => '__Directive',
                   resolve_module => ?MODULE,
                   description => "Representation of directives",
                   fields => #{
                     name => #{
                       type => 'String!',
                       description => "The name of the directive" },
                     description => #{
                       type => 'String',
                       description => "The description of the directive" },
                     locations => #{
                       type => {non_null, ['__DirectiveLocation!']},
                       description => "Where the directives can be used" },
                     onOperation => #{
                        type => 'Bool',
                        description => "Not documented yet" },
                     onFragment => #{
                        type => 'Bool',
                        description => "Not documented yet" },
                     onField => #{
                        type => 'Bool',
                        description => "Not documented yet" },
                     args => #{
                       type => {non_null, ['__InputValue!']},
                       description => "The possible arguments for the given directive" }
                    }}},
    DirectiveLocation = {enum, #{
                           id => '__DirectiveLocation',
                           description => "Where a given directive can be used",
                           resolve_module => graphql_enum_coerce,
                           values => #{
                             'QUERY' => #{ value => 1, description => "Queries" },
                             'MUTATION' => #{ value => 2, description => "Mutations" },
                             'FIELD' => #{ value => 3, description => "Fields" },
                             'FRAGMENT_DEFINITION' => #{ value => 4, description => "Fragment definitions" },
                             'FRAGMENT_SPREAD' => #{ value => 5, description => "Fragment spread" },
                             'INLINE_FRAGMENT' => #{ value => 6, description => "Inline fragments" }
                            }}},
    ok = graphql:insert_schema_definition(Namespace, DirectiveLocation),
    ok = graphql:insert_schema_definition(Namespace, Directive),
    ok = graphql:insert_schema_definition(Namespace, TypeKind),
    ok = graphql:insert_schema_definition(Namespace, Enum),
    ok = graphql:insert_schema_definition(Namespace, InputValue),
    ok = graphql:insert_schema_definition(Namespace, Field),
    ok = graphql:insert_schema_definition(Namespace, Type),
    ok = graphql:insert_schema_definition(Namespace, Schema),
    ok.

%% @todo: Look up the directive in the schema and then use that lookup as a way to render the
%% following part. Most notably, locations can be mapped from the directive type.
directive(Namespace, Kind) ->
    {Name, Desc} =
        case Kind of
            include ->
                {<<"include">>,
                 <<"include a selection on a conditional variable">> };
            skip ->
                {<<"skip">>,
                 <<"exclude a selection on a conditional variable">>}
        end,
    {ok, Bool} = render_type(Namespace, <<"Bool">>),

    #{
       <<"name">> => Name,
       <<"description">> => Desc,
       <<"locations">> =>
           [<<"FIELD">>, <<"FRAGMENT_SPREAD">>, <<"INLINE_FRAGMENT">>],
       <<"args">> =>
           [#{ <<"name">> => <<"if">>,
               <<"description">> => <<"flag for the condition">>,
               <<"type">> => Bool,
               <<"defaultValue">> => false }]
     }.

%% Resolver for introspection
execute(_Ctx, null, _, _) ->
    {ok, null};
execute(_Ctx, Obj, FieldName, _Args) ->
    case maps:get(FieldName, Obj, not_found) of
        {'$lazy', F} when is_function(F, 0) -> F();
        not_found -> {error, not_found};
        Values when is_list(Values) -> {ok, [ {ok, R} || R <- Values ]};
        Value -> {ok, Value}
    end.
