-module(riakou).

%% api
-export([start/0]).
-export([start_link/1]).
-export([start_link/2]).
-export([start_link/3]).
-export([start_link/4]).
-export([start_link/5]).
-export([start_link/6]).
-export([wait_for_connection/0]).
-export([wait_for_connection/1]).
-export([take/0]).
-export([take/1]).
-export([return/1]).
-export([return/2]).
-export([do/1]).
-export([do/2]).
-export([do/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_GROUP, ?MODULE).
-define(DEFAULT_PORT, 8087).
-define(DEFAULT_MIN, 5).
-define(DEFAULT_MAX, 100).

%% api

start() ->
  application:start(pooler),
  application:start(riakou).

start_link(URL) when is_binary(URL) ->
  {Host, Port} = parse_url(URL),
  start_link(Host, Port);
start_link(Host) ->
  start_link(Host, ?DEFAULT_PORT).

start_link(URL, Opts) when is_binary(URL) ->
  {Host, Port} = parse_url(URL),
  start_link(Host, Port, Opts);
start_link(Group, URL) when is_atom(Group), is_binary(URL) ->
  {Host, Port} = parse_url(URL),
  start_link(Group, Host, Port);
start_link(Host, Port) when is_integer(Port) ->
  start_link(Host, Port, []).

start_link(Group, URL, Opts) when is_atom(Group), is_binary(URL) ->
  {Host, Port} = parse_url(URL),
  start_link(Group, Host, Port, Opts);
start_link(Group, Host, Port) when is_atom(Group), is_integer(Port) ->
  start_link(Group, Host, Port, []);
start_link(Host, Port, Opts) when is_integer(Port) ->
  start_link(Host, Port, Opts, ?DEFAULT_MIN, ?DEFAULT_MAX).

start_link(URL, Opts, Min, Max) when is_binary(URL) ->
  {Host, Port} = parse_url(URL),
  start_link(Host, Port, Opts, Min, Max);
start_link(Group, Host, Port, Opts) when is_atom(Group) ->
  start_link(Group, Host, Port, Opts, ?DEFAULT_MIN, ?DEFAULT_MAX).

start_link(Group, URL, Opts, Min, Max) when is_atom(Group), is_binary(URL) ->
  {Host, Port} = parse_url(URL),
  start_link(Group, Host, Port, Opts, Min, Max);
start_link(Host, Port, Opts, Min, Max) ->
  start_link(?DEFAULT_GROUP, Host, Port, Opts, Min, Max).

start_link(Group, Host, Port, Opts, Min, Max) ->
  riakou_server:add(Group, Host, Port, Opts, Min, Max).

wait_for_connection() ->
  wait_for_connection(?DEFAULT_GROUP).

wait_for_connection(Group) ->
  case ?MODULE:take(Group) of
    Pid when is_pid(Pid) ->
      ?MODULE:return(Group, Pid),
      ok;
    _ ->
      timer:sleep(5),
      ?MODULE:wait_for_connection()
  end.

parse_url(URI) ->
  {ok, {_Scheme, _UserInfo, Host, Port, _Path, _Query}} = http_uri:parse(binary_to_list(URI), [{scheme_defaults, [{riak, ?DEFAULT_PORT}]}]),
  {Host, Port}.

take() ->
  take(?DEFAULT_GROUP).

take(Group) ->
  pooler:take_group_member(Group).

return(Pid) ->
  return(?DEFAULT_GROUP, Pid).

return(Group, Pid) ->
  pooler:return_group_member(Group, Pid).

do(Fun) ->
  do(?DEFAULT_GROUP, Fun).

do(Group, Fun) when is_function(Fun) ->
  case take(Group) of
    error_no_members ->
      {error, no_connections};
    {error_no_group, _} = Err ->
       Err;
    Pid ->
      try Fun(Pid)
      catch
        _:E -> E
      after
        return(Group, Pid)
      end
  end;
do(Fun, Opts) ->
  do(?DEFAULT_GROUP, Fun, Opts).

do(Group, Fun, Opts) ->
  do(Group, fun(P) ->
    apply(riakc_pb_socket, Fun, [P|Opts])
  end).

