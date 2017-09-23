-module(avro_schema_store_gen).
-behaviour(gen_server).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3]).

-export([ start/0
        , store/2
        , lookup/1
        ]).

% These are all wrappers for calls to the server
start() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
store(Name, Type) -> gen_server:call(?MODULE, {store, Name, Type}).
lookup(Name) -> gen_server:call(?MODULE, {lookup, Name}).

% This is called when a connection is made to the server
init([]) ->
  SchemaStore = dict:new(),
  {ok, SchemaStore}.

% handle_call is invoked in response to gen_server:call
handle_call({store, Name, Type}, _From, SchemaStore) ->
  NewSchemaStore = dict:append(Name, Type, SchemaStore),
  {reply, ok, NewSchemaStore};

handle_call({lookup, Name}, _From, SchemaStore) ->
  Response = case dict:find(Name, SchemaStore) of
               error -> {name_not_found, Name};
               {ok, Type} -> {type, lists:nth(1, Type)}
             end,
  {reply, Response, SchemaStore};

handle_call(_Message, _From, SchemaStore) ->
  {reply, error, SchemaStore}.

% We get compile warnings from gen_server unless we define these
handle_cast(_Message, SchemaStore) -> {noreply, SchemaStore}.
handle_info(_Message, SchemaStore) -> {noreply, SchemaStore}.
terminate(_Reason, _SchemaStore) -> ok.
code_change(_OldVersion, SchemaStore, _Extra) -> {ok, SchemaStore}.
