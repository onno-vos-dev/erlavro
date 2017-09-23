-module(avro_schema_store_server).

-export([ start/0
        , store/2
        , lookup/1
        ]).

start() ->
  register(schema_store_server, spawn(fun() ->
                                          schema_store_server(dict:new())
                                      end)).

schema_store_server(SchemaStore) ->
  receive
    {lookup, From, Name} ->
      case dict:find(Name, SchemaStore) of
        error -> From ! {name_not_found, Name};
        {ok, Type} -> From ! {type, lists:nth(1, Type)}
      end,
      schema_store_server(SchemaStore);
    {store, From, Name, Type} ->
      NewSchemaStore = dict:append(Name, Type, SchemaStore),
      From ! ok,
      schema_store_server(NewSchemaStore);
    _ -> schema_store_server ! schema_store_server(SchemaStore)
  end.

store(Name, Type) ->
  schema_store_server ! {store, self(), Name, Type},
  receive
    ok -> ok
  end.

lookup(Name) ->
  schema_store_server ! {lookup, self(), Name},
    receive
      {name_not_found, Name} ->
        {name_not_found, Name};
      {type, Type} ->
        {type, Type};
      What -> {received_weird_stuff, What}
    end.
