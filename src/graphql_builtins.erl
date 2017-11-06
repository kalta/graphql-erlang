-module(graphql_builtins).

-include("graphql_schema.hrl").

-export([directive_schema/2]).
-export([standard_types_inject/1]).

-spec standard_types_inject(graphql:namespace()) -> ok.
standard_types_inject(Namespace) ->
    String = {scalar, #{
    	id => 'String',
    	description => "UTF-8 Text Strings" }},
    Float = {scalar, #{
    	id => 'Float',
    	description => "Floating point values, IEEE 754, but not ±infty, nor NaN" }},
    Int = {scalar, #{
    	id => 'Int',
    	description => "Integers in the range ±2^53. The GraphQL standard only allows for 32-bit signed integers, but we can support up to the larger range." }},
    Bool = {scalar, #{
    	id => 'Bool',
    	description => "Boolean values, either given as the *true* and *false* values of JSON, or as the strings \"true\" and \"false\"." }},
    ID = {scalar, #{
    	id => 'ID',
    	description => "Representation of an opaque ID in the system. Always returned/given as strings, but clients are not allowed to deconstruct them. The server might change them as it sees fit later on, and the clients must be able to handle this situation." }},
    ok = graphql:insert_schema_definition(Namespace, String),
    ok = graphql:insert_schema_definition(Namespace, Float),
    ok = graphql:insert_schema_definition(Namespace, Int),
    ok = graphql:insert_schema_definition(Namespace, Bool),
    ok = graphql:insert_schema_definition(Namespace, ID),
    ok.

%% Construct schema types for the directives the system supports
directive_schema(Namespace, include) ->
    #directive_type {
       id = <<"include">>,
       locations = [field, fragment_spread, inline_fragment],
       args = #{
         <<"if">> =>
             #schema_arg{
                ty = graphql_schema:get(Namespace, <<"Bool">>),
                default = false,
                description = <<"Wether or not the item should be included">> }
        }};
directive_schema(Namespace, skip) ->
    #directive_type {
       id = <<"skip">>,
       locations = [field, fragment_spread, inline_fragment],
       args = #{
         <<"if">> =>
             #schema_arg{
                ty = graphql_schema:get(Namespace, <<"Bool">>),
                default = false,
                description = <<"Wether or not the item should be skipped">> }
        }}.

