-module(graphql_schema_validate).

-include("graphql_schema.hrl").

-export([x/1]).

-spec x(graphql:namespace()) -> ok.
x(Namespace) ->
    Objects = graphql_schema:all(Namespace),
    try
        [x(Namespace, Obj) || Obj <- Objects],
        ok
    catch
        throw:Error ->
            %% These errors are usually a bug in the programmers code,
            %% hence they are written to the error_logger as well so
            %% you get nice error messages for them apart from the
            %% Erlang term.
            error_logger:error_msg(format_error(Error)),
            exit(Error)
    end.

x(Namespace, Obj) ->
    try validate(Namespace, Obj) of
        ok -> ok
    catch
        throw:{invalid, Reason} ->
            throw({schema_validation, graphql_schema:id(Obj), Reason})
    end.


validate(_Namespace, #scalar_type {}) -> ok;
validate(Namespace, #root_schema {} = X) -> root_schema(Namespace, X);
validate(Namespace, #object_type {} = X) -> object_type(Namespace, X);
validate(_Namespace, #enum_type {} = X) -> enum_type(X);
validate(Namespace, #interface_type {} = X) -> interface_type(Namespace, X);
validate(Namespace, #union_type {} = X) -> union_type(Namespace, X);
validate(Namespace, #input_object_type {} = X) -> input_object_type(Namespace, X).

enum_type(#enum_type {}) ->
    %% TODO: Validate values
    ok.

input_object_type(Namespace, #input_object_type { fields = FS }) ->
    all(Namespace, fun schema_input_type_arg/2, maps:to_list(FS)),
    ok.

union_type(Namespace, #union_type { types = Types }) ->
    all(Namespace, fun is_union_type/2, Types),
    ok.

interface_type(Namespace, #interface_type { fields= FS }) ->
    all(Namespace, fun schema_field/2, maps:to_list(FS)),
    ok.

object_type(Namespace, #object_type {
	fields = FS,
	interfaces = IFaces} = Obj) ->
    all(Namespace, fun is_interface/2, IFaces),
    all(Namespace, fun(IF) -> implements(lookup(Namespace, IF), Obj) end, IFaces),
    all(Namespace, fun schema_field/2, maps:to_list(FS)),
    ok.

root_schema(Namespace, #root_schema {
	query = Q,
	mutation = M,
	subscription = S,
	interfaces = IFaces }) ->
    undefined_object(Namespace, Q),
    undefined_object(Namespace, M),
    undefined_object(Namespace, S),
    all(Namespace, fun is_interface/2, IFaces),
    ok.
    
schema_field(Namespace, {_, #schema_field { ty = Ty, args = Args }}) ->
    all(Namespace, fun schema_input_type_arg/2, maps:to_list(Args)),
    type(Namespace, Ty),
    ok.

schema_input_type_arg(Namespace, {_, #schema_arg { ty = Ty }}) ->
    %% TODO: Default check!
    input_type(Namespace, Ty),
    ok.

undefined_object(_Namespace, undefined) -> ok;
undefined_object(Namespace, Obj) -> is_object(Namespace, Obj).

implements(#interface_type { fields = IFFields } = IFace,
           #object_type { fields = ObjFields }) ->
    IL = lists:sort(maps:to_list(IFFields)),
    OL = lists:sort(maps:to_list(ObjFields)),
    case implements_field_check(IL, OL) of
        ok ->
            ok;
        {error, Reason} ->
            err({implements, graphql_schema:id(IFace), Reason})
    end.
    
implements_field_check([], []) -> ok;
implements_field_check([], [_|OS]) -> implements_field_check([], OS);
implements_field_check([{K, IF} | IS], [{K, OF} | OS]) ->
    %% TODO: Arg check!
    IFType = IF#schema_field.ty,
    OFType = OF#schema_field.ty,
    case IFType == OFType of
        true ->
            implements_field_check(IS, OS);
        false ->
            {error, {type_mismatch, #{ key => K,
                                       interface => IFType,
                                       object => OFType }}}
    end;
implements_field_check([{IK, _} | _] = IL, [{OK, _} | OS]) when IK > OK ->
    implements_field_check(IL, OS);
implements_field_check([{IK, _} | _], [{OK, _} | _]) when IK < OK ->
    {error, {field_not_found_in_object, IK}}.
    
is_interface(Namespace, IFace) ->
    case lookup(Namespace, IFace) of
        #interface_type{} -> ok;
        _ -> err({not_interface, IFace})
    end.

is_object(Namespace, Obj) ->
    case lookup(Namespace, Obj) of
        #object_type{} -> ok;
        _ -> err({not_object, Obj})
    end.

is_union_type(Namespace, Obj) ->
    case lookup(Namespace, Obj) of
        #object_type{} -> ok;
        _ -> err({not_union_type, Obj})
    end.

type(Namespace, {non_null, T}) -> type(Namespace, T);
type(Namespace, {list, T}) -> type(Namespace, T);
type(Namespace, X) when is_binary(X) ->
    case lookup(Namespace, X) of
        #input_object_type {} ->
            err({invalid_output_type, X});

        _ ->
            ok
    end.

input_type(Namespace, {non_null, T}) -> input_type(Namespace, T);
input_type(Namespace, {list, T}) -> input_type(Namespace, T);
input_type(Namespace, X) when is_binary(X) ->
    case lookup(Namespace, X) of
        #input_object_type {} -> ok;
        #enum_type {} -> ok;
        #scalar_type {} -> ok;
        _V ->
            err({invalid_input_type, X})
    end.

all(_Namespace, _F, []) -> ok;
all(Namespace, F, [E|Es]) ->
    ok = F(Namespace, E),
    all(Namespace, F, Es).

lookup(Namespace, Key) ->
    case graphql_schema:lookup(Namespace, Key) of
        not_found -> err({not_found, Key});
        X -> X
    end.
    
err(Reason) -> throw({invalid, Reason}).

format_error(X) -> binary_to_list(
                     iolist_to_binary(
                       err_fmt(X))).

err_fmt({schema_validation, Type, {not_found, NF}}) ->
    io_lib:format(
      "Schema Error in type ~p: it refers to a type ~p, "
      "which is not present in the schema", [Type, NF]);
err_fmt(X) ->
    io_lib:format(
      "Unhandled schema validator error message: ~p", [X]).
