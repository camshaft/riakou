-module(riakou_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%% API functions

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
  RiakouServer = {riakou_server, {riakou_server, start_link, []},
            permanent, 2000, worker, [riakou_server]},

  {ok, {{one_for_one, 1000, 3600}, [RiakouServer]}}.

