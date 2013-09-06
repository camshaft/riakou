-module(riakou).

%% api

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

start_link(Host)->
  start_link(Host, 8087).

start_link(Host, Port)->
  start_link(Host, Port, []).

start_link(Host, Port, Opts)->
  start_link(Host, Port, Opts, 5, 10).

start_link(Host, Port, Opts, Min, Max)->
  start_link(?MODULE, Host, Port, Opts, Min, Max).

start_link(Group, Host, Port, Opts, Min, Max)->
  %% Get a list of all of the riak nodes
  {ok, IPList} = inet:getaddrs(Host, inet),

  _Pools = [begin
    PoolOpts = [
      {name, pool_name(Group, IP, Port)},
      {group, Group},
      {max_count, Max},
      {init_count, Min},
      {start_mfa, {riakc_pb_socket, start_link, [IP, Port, Opts]}}
    ],
    pooler:new_pool(PoolOpts)
  end || IP <- IPList],

  %% TODO spawn a process to poll the dns entry and update the pools from the ip list

  ok.

take()->
  take(?MODULE).

take(Group)->
  pooler:take_group_member(Group).

return(Pid)->
  return(?MODULE, Pid).

return(Group, Pid)->
  pooler:return_group_member(Group, Pid).

do(Fun)->
  do(?MODULE, Fun).

do(Pool, Fun)->
  poolboy:transaction(Pool, Fun).

pool_name(Group, {IP1, IP2, IP3, IP4}, Port)->
  IP = string:join([
    integer_to_list(Int)
  || Int <- [IP1, IP2, IP3, IP4]], "."),
  
  list_to_atom(atom_to_list(Group) ++ "://" ++ IP ++ ":" ++ integer_to_list(Port)).
