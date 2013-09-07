-module(riakou).

%% api
-export([start/0]).
-export([start_link/1]).
-export([start_link/2]).
-export([start_link/3]).
-export([start_link/5]).
-export([start_link/6]).
-export([take/0]).
-export([take/1]).
-export([return/1]).
-export([return/2]).
-export([do/1]).
-export([do/2]).

-define(SERVER, ?MODULE).

%% api

start() ->
  application:start(pooler),
  application:start(riakou).

start_link(Host) ->
  start_link(Host, 8087).

start_link(Host, Port) ->
  start_link(Host, Port, []).

start_link(Host, Port, Opts) ->
  start_link(Host, Port, Opts, 5, 10).

start_link(Host, Port, Opts, Min, Max) ->
  start_link(?MODULE, Host, Port, Opts, Min, Max).

start_link(Group, Host, Port, Opts, Min, Max) ->
  riakou_server:add(Group, Host, Port, Opts, Min, Max).

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

do(Group, Fun) ->
  Pid = take(Group),
  try Fun(Pid)
  catch
    _:E -> E
  after
    return(Group, Pid)
  end.

