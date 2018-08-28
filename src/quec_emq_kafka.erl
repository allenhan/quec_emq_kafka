%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(quec_emq_kafka).

-include_lib("emqttd/include/emqttd.hrl").

-define(APP, quec_emq_kafka).

-export([load/0, unload/0]).

-export([on_client_connected/3, on_client_disconnected/3]).

-export([on_client_subscribe/4, on_client_unsubscribe/4]).

-export([on_session_created/3, on_session_subscribed/4, on_session_unsubscribed/4,
         on_session_terminated/4]).

-export([on_message_publish/2, on_message_delivered/4, on_message_acked/4]).

-define(LOG(Level, Format, Args), lager:Level("WebHook: " ++ Format, Args)).

load() ->
    %ekaf_init(application:get_all_env()),
    lists:foreach(fun({Hook, Fun, Filter}) ->
        load_(Hook, binary_to_atom(Fun, utf8), Filter, {Filter})
    end, parse_rule(application:get_env(?APP, rules, []))).

unload() ->
    lists:foreach(fun({Hook, Fun, Filter}) ->
        unload_(Hook, binary_to_atom(Fun, utf8), Filter)
    end, parse_rule(application:get_env(?APP, rules, []))).

%%--------------------------------------------------------------------
%% Client connected
%%--------------------------------------------------------------------

on_client_connected(0, Client = #mqtt_client{client_id = ClientId, username = Username}, _Env) ->
    Params = [{action, client_connected},
              {client_id, ClientId},
              {username, Username},
              {conn_ack, 0}],
     send_http_request(Params),

    {ok, Client};

on_client_connected(_, Client = #mqtt_client{}, _Env) ->
    {ok, Client}.

%%--------------------------------------------------------------------
%% Client disconnected
%%--------------------------------------------------------------------

on_client_disconnected(auth_failure, #mqtt_client{}, Env) ->
    ok;
on_client_disconnected({shutdown, Reason}, Client, Env) when is_atom(Reason) ->
    on_client_disconnected(Reason, Client, Env);
on_client_disconnected(Reason, _Client = #mqtt_client{client_id = ClientId, username = Username}, _Env)
    when is_atom(Reason) ->
    Params = [{action, client_disconnected},
              {client_id, ClientId},
              {username, Username},
              {reason, Reason}],
    send_http_request(Params),
    ok;
on_client_disconnected(Reason, _Client, _Env) ->
    ?LOG(error, "Client disconnected, cannot encode reason: ~p", [Reason]),
    ok.

%%--------------------------------------------------------------------
%% Client subscribe
%%--------------------------------------------------------------------

on_client_subscribe(ClientId, Username, TopicTable, {Filter}) ->
    lists:foreach(fun({Topic, Opts}) ->
        with_filter(fun() ->
            Params = [{action, client_subscribe},
                      {client_id, ClientId},
                      {username, Username},
                      {topic, Topic},
                      {opts, Opts}],
            send_http_request(Params)
        end, Topic, Filter)
    end, TopicTable).

%%--------------------------------------------------------------------
%% Client unsubscribe
%%--------------------------------------------------------------------

on_client_unsubscribe(ClientId, Username, TopicTable, {Filter}) ->
    lists:foreach(fun({Topic, Opts}) ->
        with_filter(fun() ->
            Params = [{action, client_unsubscribe},
                      {client_id, ClientId},
                      {username, Username},
                      {topic, Topic},
                      {opts, Opts}],
            send_http_request(Params)
        end, Topic, Filter)
    end, TopicTable).

%%--------------------------------------------------------------------
%% Session created
%%--------------------------------------------------------------------

on_session_created(ClientId, Username, _Env) ->
    Params = [{action, session_created},
              {client_id, ClientId},
              {username, Username}],
    send_http_request(Params),
    ok.

%%--------------------------------------------------------------------
%% Session subscribed
%%--------------------------------------------------------------------

on_session_subscribed(ClientId, Username, {Topic, Opts}, {Filter}) ->
    with_filter(fun() ->
        Params = [{action, session_subscribed},
                  {client_id, ClientId},
                  {username, Username},
                  {topic, Topic},
                  {opts, Opts}],
        send_http_request(Params)
    end, Topic, Filter).

%%--------------------------------------------------------------------
%% Session unsubscribed
%%--------------------------------------------------------------------

on_session_unsubscribed(ClientId, Username, {Topic, _Opts}, {Filter}) ->
    with_filter(fun() ->
        Params = [{action, session_unsubscribed},
                  {client_id, ClientId},
                  {username, Username},
                  {topic, Topic}],
        send_http_request(Params)
    end, Topic, Filter).

%%--------------------------------------------------------------------
%% Session terminated
%%--------------------------------------------------------------------

on_session_terminated(ClientId, Username, {shutdown, Reason}, Env) when is_atom(Reason) ->
    on_session_terminated(ClientId, Username, Reason, Env);
on_session_terminated(ClientId, Username, Reason, _Env) when is_atom(Reason) ->
    Params = [{action, session_terminated},
              {client_id, ClientId},
              {username, Username},
              {reason, Reason}],
    send_http_request(Params),
    ok;
on_session_terminated(_ClientId, _Username, Reason, _Env) ->
    ?LOG(error, "Session terminated, cannot encode the reason: ~p", [Reason]),
    ok.

%%--------------------------------------------------------------------
%% Message publish
%%--------------------------------------------------------------------

on_message_publish(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};
on_message_publish(Message = #mqtt_message{topic = Topic}, {Filter}) ->
    with_filter(fun() ->
        {FromClientId, FromUsername} = format_from(Message#mqtt_message.from),
        Params = [{action, message_publish},
                  {from_client_id, FromClientId},
                  {from_username, FromUsername},
                  {topic, Message#mqtt_message.topic},
                  {qos, Message#mqtt_message.qos},
                  {retain, Message#mqtt_message.retain},
                  {payload, Message#mqtt_message.payload},
                  {ts, emqttd_time:now_secs(Message#mqtt_message.timestamp)}],
        send_http_request(Params),
        {ok, Message}
    end, Message, Topic, Filter).

%%--------------------------------------------------------------------
%% Message delivered
%%--------------------------------------------------------------------

on_message_delivered(ClientId, Username, Message = #mqtt_message{topic = Topic}, {Filter}) ->
    with_filter(fun() ->
        {FromClientId, FromUsername} = format_from(Message#mqtt_message.from),
        Params = [{action, message_delivered},
                  {client_id, ClientId},
                  {username, Username},
                  {from_client_id, FromClientId},
                  {from_username, FromUsername},
                  {topic, Message#mqtt_message.topic},
                  {qos, Message#mqtt_message.qos},
                  {retain, Message#mqtt_message.retain},
                  {payload, Message#mqtt_message.payload},
                  {ts, emqttd_time:now_secs(Message#mqtt_message.timestamp)}],
        send_http_request(Params)
    end, Topic, Filter).

%%--------------------------------------------------------------------
%% Message acked
%%--------------------------------------------------------------------

on_message_acked(ClientId, Username, Message = #mqtt_message{topic = Topic}, {Filter}) ->
    with_filter(fun() ->
        {FromClientId, FromUsername} = format_from(Message#mqtt_message.from),
        Params = [{action, message_acked},
                  {client_id, ClientId},
                  {username, Username},
                  {from_client_id, FromClientId},
                  {from_username, FromUsername},
                  {topic, Message#mqtt_message.topic},
                  {qos, Message#mqtt_message.qos},
                  {retain, Message#mqtt_message.retain},
                  {payload, Message#mqtt_message.payload},
                  {ts, emqttd_time:now_secs(Message#mqtt_message.timestamp)}],
        send_http_request(Params)
    end, Topic, Filter).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

send_http_request(Params) ->
    %Params1 = iolist_to_binary(mochijson2:encode(Params)),
    Params1 = mochijson2:encode(Params).
   % ekaf_send_sync(Params1).

%%    Url = application:get_env(?APP, url, "http://127.0.0.1"),
%%    ?LOG(debug, "Url:~p, params:~s", [Url, Params1]),
%%    case request_(post, {Url, [], "application/json", Params1}, [{timeout, 5000}], [], 0) of
%%        {ok, _} ->
%%            ok;
%%        {error, Reason} ->
%%            ?LOG(error, "HTTP request error: ~p", [Reason]), ok %% TODO: return ok?
%%    end.

request_(Method, Req, HTTPOpts, Opts, Times) ->
    %% Resend request, when TCP closed by remotely
    case httpc:request(Method, Req, HTTPOpts, Opts) of
        {error, socket_closed_remotely} when Times < 3 ->
            timer:sleep(trunc(math:pow(10, Times))),
            request_(Method, Req, HTTPOpts, Opts, Times+1);
        Other -> Other
    end.

parse_rule(Rules) ->
    parse_rule(Rules, []).
parse_rule([], Acc) ->
    lists:reverse(Acc);
parse_rule([{Rule, Conf} | Rules], Acc) ->
    {_, Params} = mochijson2:decode(Conf),
    Action = proplists:get_value(<<"action">>, Params),
    Filter = proplists:get_value(<<"topic">>, Params),
    parse_rule(Rules, [{list_to_atom(Rule), Action, Filter} | Acc]).

with_filter(Fun, _, undefined) ->
    Fun(), ok;
with_filter(Fun, Topic, Filter) ->
    case emqttd_topic:match(Topic, Filter) of
        true  -> Fun(), ok;
        false -> ok
    end.

with_filter(Fun, _, _, undefined) ->
    Fun();
with_filter(Fun, Msg, Topic, Filter) ->
    case emqttd_topic:match(Topic, Filter) of
        true  -> Fun();
        false -> {ok, Msg}
    end.

format_from({ClientId, Username}) ->
    {ClientId, Username};
format_from(From) when is_atom(From) ->
    {a2b(From), a2b(From)};
format_from(_) ->
    {<<>>, <<>>}.

a2b(A) -> erlang:atom_to_binary(A, utf8).



load_(Hook, Fun, Filter, Params) ->
    case Hook of
        'client.connected'    -> emqttd:hook(Hook, {Filter, fun ?MODULE:Fun/3}, [Params]);
        'client.disconnected' -> emqttd:hook(Hook, {Filter, fun ?MODULE:Fun/3}, [Params]);
        'client.subscribe'    -> emqttd:hook(Hook, {Filter, fun ?MODULE:Fun/4}, [Params]);
        'client.unsubscribe'  -> emqttd:hook(Hook, {Filter, fun ?MODULE:Fun/4}, [Params]);
        'session.created'     -> emqttd:hook(Hook, {Filter, fun ?MODULE:Fun/3}, [Params]);
        'session.subscribed'  -> emqttd:hook(Hook, {Filter, fun ?MODULE:Fun/4}, [Params]);
        'session.unsubscribed'-> emqttd:hook(Hook, {Filter, fun ?MODULE:Fun/4}, [Params]);
        'session.terminated'  -> emqttd:hook(Hook, {Filter, fun ?MODULE:Fun/4}, [Params]);
        'message.publish'     -> emqttd:hook(Hook, {Filter, fun ?MODULE:Fun/2}, [Params]);
        'message.acked'       -> emqttd:hook(Hook, {Filter, fun ?MODULE:Fun/4}, [Params]);
        'message.delivered'   -> emqttd:hook(Hook, {Filter, fun ?MODULE:Fun/4}, [Params])
    end.

unload_(Hook, Fun, Filter) ->
    case Hook of
        'client.connected'    -> emqttd:unhook(Hook, {Filter, fun ?MODULE:Fun/3});
        'client.disconnected' -> emqttd:unhook(Hook, {Filter, fun ?MODULE:Fun/3});
        'client.subscribe'    -> emqttd:unhook(Hook, {Filter, fun ?MODULE:Fun/4});
        'client.unsubscribe'  -> emqttd:unhook(Hook, {Filter, fun ?MODULE:Fun/4});
        'session.created'     -> emqttd:unhook(Hook, {Filter, fun ?MODULE:Fun/3});
        'session.subscribed'  -> emqttd:unhook(Hook, {Filter, fun ?MODULE:Fun/4});
        'session.unsubscribed'-> emqttd:unhook(Hook, {Filter, fun ?MODULE:Fun/4});
        'session.terminated'  -> emqttd:unhook(Hook, {Filter, fun ?MODULE:Fun/4});
        'message.publish'     -> emqttd:unhook(Hook, {Filter, fun ?MODULE:Fun/2});
        'message.acked'       -> emqttd:unhook(Hook, {Filter, fun ?MODULE:Fun/4});
        'message.delivered'   -> emqttd:unhook(Hook, {Filter, fun ?MODULE:Fun/4})
    end.

%% ==================== ekaf_init STA.===============================%%

%%ekaf_init(_Env) ->
%%
%%  {ok, Kafka} = application:get_env(emqttd_plugin_kafka_bridge, kafka),
%%  BootstrapBroker = proplists:get_value(bootstrap_broker, Kafka),
%%  PartitionStrategy= proplists:get_value(partition_strategy, Kafka),
%%  %% Set partition strategy, like application:set_env(ekaf, ekaf_partition_strategy, strict_round_robin),
%%  application:set_env(ekaf, ekaf_partition_strategy, PartitionStrategy),
%%  %% Set broker url and port, like application:set_env(ekaf, ekaf_bootstrap_broker, {"127.0.0.1", 9092}),
%%  application:set_env(ekaf, ekaf_bootstrap_broker, BootstrapBroker),
%%  %% Set topic
%%  application:set_env(ekaf, ekaf_bootstrap_topics, <<"quec_emq_to_kafka">>),
%%
%%%%  {ok, _} = application:ensure_all_started(kafkamocker),
%%%%  {ok, _} = application:ensure_all_started(gproc),
%%%%  {ok, _} = application:ensure_all_started(ranch),
%%%%  {ok, _} = application:ensure_all_started(ekaf),
%%
%%  io:format("Init ekaf with ~p~n", [BootstrapBroker]),
%%
%%  ok.
%%%% ==================== ekaf_init END.===============================%%
%%
%%
%%%% ==================== ekaf_send STA.===============================%%
%%ekaf_send(Type, ClientId, {}, _Env) ->
%%  Json = mochijson2:encode([
%%    {type, Type},
%%    {client_id, ClientId},
%%    {message, {}},
%%    {cluster_node, node()},
%%    {ts, emqttd_time:now_ms()}
%%  ]),
%%  ekaf_send_sync(Json);
%%ekaf_send(Type, ClientId, {Reason}, _Env) ->
%%  Json = mochijson2:encode([
%%    {type, Type},
%%    {client_id, ClientId},
%%    {cluster_node, node()},
%%    {message, Reason},
%%    {ts, emqttd_time:now_ms()}
%%  ]),
%%  ekaf_send_sync(Json);
%%ekaf_send(Type, ClientId, {Topic, Opts}, _Env) ->
%%  Json = mochijson2:encode([
%%    {type, Type},
%%    {client_id, ClientId},
%%    {cluster_node, node()},
%%    {message, [
%%      {topic, Topic},
%%      {opts, Opts}
%%    ]},
%%    {ts, emqttd_time:now_ms()}
%%  ]),
%%  ekaf_send_sync(Json);
%%ekaf_send(Type, _, Message, _Env) ->
%%  Id = Message#mqtt_message.id,
%%  From = Message#mqtt_message.from, %需要登录和不需要登录这里的返回值是不一样的
%%  Topic = Message#mqtt_message.topic,
%%  Payload = Message#mqtt_message.payload,
%%  Qos = Message#mqtt_message.qos,
%%  Dup = Message#mqtt_message.dup,
%%  Retain = Message#mqtt_message.retain,
%%  Timestamp = Message#mqtt_message.timestamp,
%%
%%  ClientId = c(From),
%%  Username = u(From),
%%
%%  Json = mochijson2:encode([
%%    {type, Type},
%%    {client_id, ClientId},
%%    {message, [
%%      {username, Username},
%%      {topic, Topic},
%%      {payload, Payload},
%%      {qos, i(Qos)},
%%      {dup, i(Dup)},
%%      {retain, i(Retain)}
%%    ]},
%%    {cluster_node, node()},
%%    {ts, emqttd_time:now_ms()}
%%  ]),
%%  ekaf_send_sync(Json).
%%
%%ekaf_send_async(Msg) ->
%%  Topic = ekaf_get_topic(),
%%  ekaf_send_async(Topic, Msg).
%%ekaf_send_async(Topic, Msg) ->
%%  ekaf:produce_async_batched(list_to_binary(Topic), list_to_binary(Msg)).
%%ekaf_send_sync(Msg) ->
%%  Topic = ekaf_get_topic(),
%%  ekaf_send_sync(Topic, Msg).
%%ekaf_send_sync(Topic, Msg) ->
%%  ekaf:produce_sync_batched(list_to_binary(Topic), list_to_binary(Msg)).
%%
%%i(true) -> 1;
%%i(false) -> 0;
%%i(I) when is_integer(I) -> I.
%%c({ClientId, Username}) -> ClientId;
%%c(From) -> From.
%%u({ClientId, Username}) -> Username;
%%u(From) -> From.
%%%% ==================== ekaf_send END.===============================%%
%%
%%
%%%% ==================== ekaf_set_host STA.===============================%%
%%ekaf_set_host(Host) ->
%%  ekaf_set_host(Host, 9092).
%%ekaf_set_host(Host, Port) ->
%%  Broker = {Host, Port},
%%  application:set_env(ekaf, ekaf_bootstrap_broker, Broker),
%%  io:format("reset ekaf Broker ~s:~b ~n", [Host, Port]),
%%  ok.
%%%% ==================== ekaf_set_host END.===============================%%
%%
%%%% ==================== ekaf_set_topic STA.===============================%%
%%ekaf_set_topic(Topic) ->
%%  application:set_env(ekaf, ekaf_bootstrap_topics, list_to_binary(Topic)),
%%  ok.
%%ekaf_get_topic() ->
%%  Env = application:get_env(?APP, kafka),
%%  {ok, Kafka} = Env,
%%  Topic = proplists:get_value(topic, Kafka),
%%  Topic.
%% ==================== ekaf_set_topic END.===============================%%

