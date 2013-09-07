riakou
======

`riakou` is a connection pooler for riak with some nice features:

* Handles disconnection/reconnection gracefully
* Uses DNS to get a list of the available riak nodes

Usage
-----

### Staring a pool

Just call `riakou:start_link`. This function is overloaded with several options (see the source :) ).

`start_link` can called with either `inet:hostname()` or `binary()`. The binary should be in the form of a riak url (`riak://my-riak-host:1234`).

When the pool starts up it will make a DNS query to get the list of IP addresses associated with the hostname. It will poll for any changes after that so make sure it has the latest list. Using DNS makes it very easy to automate adding/removing nodes from a cluster without having to push all of the changes out to the applications or running a complicated setup like zookeeper.

`NOTE` There will be a slight gap between polling when the lists aren't the same but it shouldn't be a problem since there are pools connected to other nodes.

### Using a pool

Once we've started the pool it's easy to start calling riak:

```erlang
riakou:do(get, [<<"bucket">>, <<"key">>]).
riakou:go(list_keys, [<<"bucket">>]).
```

You can also pass a function to do more advanced operations:

```erlang
riakou:do(fun(C) ->
  [riakc_pb_socket:get(C, <<"bucket">>, Key) || Key <- [<<"1">>, <<"2">>, <<"3">>]]
end)
```
