-module(riakou_server).

-behaviour(gen_server).

%% api

-export([start_link/0]).
-export([add/6]).
-export([set_poll/1]).

%% gen_server

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(POLL_RATE, 300000). %% 5 minutes

-record(state, {
  poll_rate = ?POLL_RATE
}).

-record(pool_state, {
  port,
  host,
  group,
  max = 10,
  min = 5,
  ip_list = [],
  riak_opts = []
}).

%% api

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

add(Group, Host, Port, RiakOpts, Min, Max) ->
  gen_server:call(?SERVER, {register, Group, Host, Port, RiakOpts, Min, Max}).

set_poll(Rate) ->
  gen_server:call(?SERVER, {set_poll, Rate}).

%% gen_server

init([]) ->
  {ok, #state{}}.

handle_call({register, Group, Host, Port, RiakOpts, Min, Max}, _From, #state{poll_rate = Rate} = State) ->
  PoolState = #pool_state{
    host = Host,
    group = Group,
    port = Port,
    riak_opts = RiakOpts,
    min = Min,
    max = Max
  },
  {ok, NewPoolState} = update_pools(PoolState),
  poll(NewPoolState, Rate),
  {reply, ok, State};
handle_call(_Request, _From, State) ->
  {reply, {error, not_handled}, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({refresh_pools, PoolState}, #state{poll_rate = Rate} = State) ->
  {ok, NewPoolState} = update_pools(PoolState),
  poll(NewPoolState, Rate),
  {noreply, State};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% Internal functions

poll(PoolState, Rate) ->
  timer:send_after(Rate, {refresh_pools, PoolState}).

update_pools(#pool_state{host = Host} = PoolState) ->
  diff_ips(lookup(Host), PoolState).

diff_ips({ok, List}, #pool_state{ip_list = List} = PoolState) ->
  {ok, PoolState};
diff_ips({ok, NewList}, #pool_state{ip_list = OldList} = PoolState) ->
  NewPoolState = PoolState#pool_state{ip_list = NewList},
  add_pools(NewList, OldList, NewPoolState),
  {ok, NewPoolState};
diff_ips(_, PoolState) ->
  {ok, PoolState}.

add_pools([NewIP|NewList], OldList, PoolState) ->
  case lists:member(NewIP, OldList) of
    true ->
      add_pools(NewList, lists:delete(NewIP, OldList), PoolState);
    _ ->
      add_pool(NewIP, PoolState),
      add_pools(NewList, OldList, PoolState)
  end;
add_pools([], OldList, PoolState) ->
  clean_up_old_pools(OldList, PoolState).

clean_up_old_pools([IP|OldList], PoolState) ->
  pooler:rm_pool(pool_name(IP, PoolState)),
  clean_up_old_pools(OldList, PoolState);
clean_up_old_pools([], PoolState) ->
  {ok, PoolState}.

add_pool(IP, PoolState = #pool_state{group = Group, min = Min, max = Max, port = Port, riak_opts = RiakOpts})->
  PoolOpts = [
    {name, pool_name(IP, PoolState)},
    {group, Group},
    {max_count, Max},
    {init_count, Min},
    {start_mfa, {riakc_pb_socket, start_link, [IP, Port, RiakOpts]}}
  ],
  {ok, _} = pooler:new_pool(PoolOpts).

lookup(Host) ->
  inet:getaddrs(Host, inet).

pool_name({IP1, IP2, IP3, IP4}, #pool_state{port = Port, group = Group} = _PoolState) ->
  IP = string:join([
    integer_to_list(Int)
  || Int <- [IP1, IP2, IP3, IP4]], "."),
  list_to_atom(atom_to_list(Group) ++ "::" ++ IP ++ ":" ++ integer_to_list(Port)).

