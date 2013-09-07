-module(riakou).

%% api
-export([start/0]).
-export([start_link/1]).
-export([start_link/2]).
-export([start_link/3]).
-export([start_link/4]).
-export([start_link/5]).
-export([start_link/6]).
-export([take/0]).
-export([take/1]).
-export([return/1]).
-export([return/2]).
-export([do/1]).
-export([do/2]).

-define(SERVER, ?MODULE).
-define(DEFAULT_PORT, 8087).

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
start_link(Host, Port) ->
  start_link(Host, Port, []).

start_link(Host, Port, Opts) ->
  start_link(Host, Port, Opts, 5, 50).

start_link(URL, Opts, Min, Max) ->
  {Host, Port} = parse_url(URL),
  start_link(Host, Port, Opts, Min, Max).

start_link(Group, URL, Opts, Min, Max) when is_binary(URL) ->
  {Host, Port} = parse_url(URL),
  start_link(Group, Host, Port, Opts, Min, Max);
start_link(Host, Port, Opts, Min, Max) ->
  start_link(?MODULE, Host, Port, Opts, Min, Max).

start_link(Group, Host, Port, Opts, Min, Max) ->
  riakou_server:add(Group, Host, Port, Opts, Min, Max).

parse_url(URI) ->
  {ok, {_Scheme, _UserInfo, Host, Port, _Path, _Query}} = http_uri:parse(binary_to_list(URI), [{scheme_defaults, [{riak, ?DEFAULT_PORT}]}]),
  {Host, Port}.

take() ->
  take(?MODULE).

take(Group) ->
  pooler:take_group_member(Group).

return(Pid) ->
  return(?MODULE, Pid).

return(Group, Pid) ->
  pooler:return_group_member(Group, Pid).

do(Fun) ->
  do(?MODULE, Fun).

do(Group, Fun) when is_function(Fun) ->
  case take(Group) of
    error_no_members ->
      {error, no_connections};
    Pid ->
      try Fun(Pid)
      catch
        _:E -> E
      after
        return(Group, Pid)
      end
  end;
do(Fun, Opts) ->
  do(fun(P) ->
    apply(riakc_pb_socket, Fun, [P|Opts])
  end).

