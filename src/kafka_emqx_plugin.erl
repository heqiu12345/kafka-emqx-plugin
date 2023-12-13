-module(kafka_emqx_plugin).

%% for #message{} record
%% no need for this include if we call emqx_message:to_map/1 to convert it to a map
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").


%% for logging
-include_lib("emqx/include/logger.hrl").
-include_lib("kernel/include/logger.hrl").

-export([load/1
  , unload/0
]).

%% Client Lifecycle Hooks
-export([on_client_connect/3
  , on_client_connack/4
  , on_client_connected/3
  , on_client_disconnected/4
  , on_client_authenticate/3
  , on_client_authorize/5
  , on_client_subscribe/4
  , on_client_unsubscribe/4
]).

%% Session Lifecycle Hooks
-export([on_session_created/3
  , on_session_subscribed/4
  , on_session_unsubscribed/4
  , on_session_resumed/3
  , on_session_discarded/3
  , on_session_takenover/3
  , on_session_terminated/4
]).

%% Message Pubsub Hooks
-export([on_message_publish/2
  , on_message_puback/4
  , on_message_delivered/3
  , on_message_acked/3
  , on_message_dropped/4
]).

%% Called when the plugin application start
load(Env) ->
  kafka_init([Env]),
  hook('client.connect', {?MODULE, on_client_connect, [Env]}),
  hook('client.connack', {?MODULE, on_client_connack, [Env]}),
  hook('client.connected', {?MODULE, on_client_connected, [Env]}),
  hook('client.disconnected', {?MODULE, on_client_disconnected, [Env]}),
  hook('client.authenticate', {?MODULE, on_client_authenticate, [Env]}),
  hook('client.authorize', {?MODULE, on_client_authorize, [Env]}),
  hook('client.subscribe', {?MODULE, on_client_subscribe, [Env]}),
  hook('client.unsubscribe', {?MODULE, on_client_unsubscribe, [Env]}),
  hook('session.created', {?MODULE, on_session_created, [Env]}),
  hook('session.subscribed', {?MODULE, on_session_subscribed, [Env]}),
  hook('session.unsubscribed', {?MODULE, on_session_unsubscribed, [Env]}),
  hook('session.resumed', {?MODULE, on_session_resumed, [Env]}),
  hook('session.discarded', {?MODULE, on_session_discarded, [Env]}),
  hook('session.takenover', {?MODULE, on_session_takenover, [Env]}),
  hook('session.terminated', {?MODULE, on_session_terminated, [Env]}),
  hook('message.publish', {?MODULE, on_message_publish, [Env]}),
  hook('message.puback', {?MODULE, on_message_puback, [Env]}),
  hook('message.delivered', {?MODULE, on_message_delivered, [Env]}),
  hook('message.acked', {?MODULE, on_message_acked, [Env]}),
  hook('message.dropped', {?MODULE, on_message_dropped, [Env]}).

%%--------------------------------------------------------------------
%% Client Lifecycle Hooks
%%--------------------------------------------------------------------

on_client_connect(ConnInfo, Props, _Env) ->
  %% this is to demo the usage of EMQX's structured-logging macro
  %% * Recommended to always have a `msg` field,
  %% * Use underscore instead of space to help log indexers,
  %% * Try to use static fields
  ?SLOG(debug, #{msg => "demo_log_msg_on_client_connect",
    conninfo => ConnInfo,
    props => Props}),
  %% If you want to refuse this connection, you should return with:
  %% {stop, {error, ReasonCode}}
  %% the ReasonCode can be found in the emqx_reason_codes.erl
  {ok, Props}.

on_client_connack(ConnInfo = #{clientid := ClientId}, Rc, Props, _Env) ->
  ?LOG_INFO("Client(~s) connack, ConnInfo: ~p, Rc: ~p, Props: ~p~n",
    [ClientId, ConnInfo, Rc, Props]),
  {ok, Props}.

on_client_connected(ClientInfo = #{clientid := ClientId}, ConnInfo, _Env) ->
  ?LOG_INFO("Client(~s) connected, ClientInfo:~n~p~n, ConnInfo:~n~p~n",
    [ClientId, ClientInfo, ConnInfo]),
  {IpAddr, _Port} = maps:get(peername, ConnInfo),
  Action = <<"connected">>,
  Now = now_mill_secs(os:timestamp()),
  Online = 1,
  Payload = [
    {action, Action},
    {device_id, ClientId},
    {username, maps:get(username, ClientInfo)},
    {keepalive, maps:get(keepalive, ConnInfo)},
    {ipaddress, iolist_to_binary(ntoa(IpAddr))},
    {proto_name, maps:get(proto_name, ConnInfo)},
    {proto_ver, maps:get(proto_ver, ConnInfo)},
    {ts, Now},
    {online, Online}
  ],
  produce_kafka_payload(ClientId, Payload),
  ok.

on_client_disconnected(ClientInfo = #{clientid := ClientId}, ReasonCode, ConnInfo, _Env) ->
  ?LOG_INFO("Client(~s) disconnected due to ~p, ClientInfo:~n~p~n, ConnInfo:~n~p~n",
    [ClientId, ReasonCode, ClientInfo, ConnInfo]),
  Action = <<"disconnected">>,
  Now = now_mill_secs(os:timestamp()),
  Online = 0,
  Payload = [
    {action, Action},
    {device_id, ClientId},
    {username, maps:get(username, ClientInfo)},
    {reason, ReasonCode},
    {ts, Now},
    {online, Online}
  ],
  produce_kafka_payload(ClientId, Payload),
  ok.

on_client_authenticate(ClientInfo = #{clientid := ClientId}, Result, Env) ->
  ?LOG_INFO("Client(~s) authenticate, ClientInfo:~n~p~n, Result:~p,~nEnv:~p~n",
    [ClientId, ClientInfo, Result, Env]),
  {ok, Result}.

on_client_authorize(ClientInfo = #{clientid := ClientId}, PubSub, Topic, Result, Env) ->
  ?LOG_INFO("Client(~s) authorize, ClientInfo:~n~p~n, ~p to topic(~s) Result:~p,~nEnv:~p~n",
    [ClientId, ClientInfo, PubSub, Topic, Result, Env]),
  {ok, Result}.

on_client_subscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
  ?LOG_INFO("Client(~s) will subscribe: ~p~n", [ClientId, TopicFilters]),
  Topic = erlang:element(1, erlang:hd(TopicFilters)),
  Qos = erlang:element(2, lists:last(TopicFilters)),
  Action = <<"subscribe">>,
  Now = now_mill_secs(os:timestamp()),
  Payload = [
    {device_id, ClientId},
    {action, Action},
    {topic, Topic},
    {qos, maps:get(qos, Qos)},
    {ts, Now}
  ],
  produce_kafka_payload(ClientId, Payload),
  {ok, TopicFilters}.

on_client_unsubscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
  ?LOG_INFO("Client(~s) will unsubscribe ~p~n", [ClientId, TopicFilters]),
  Topic = erlang:element(1, erlang:hd(TopicFilters)),
  Action = <<"unsubscribe">>,
  Now = now_mill_secs(os:timestamp()),
  Payload = [
    {device_id, ClientId},
    {action, Action},
    {topic, Topic},
    {ts, Now}
  ],
  produce_kafka_payload(ClientId, Payload),
  {ok, TopicFilters}.

%%--------------------------------------------------------------------
%% Session Lifecycle Hooks
%%--------------------------------------------------------------------

on_session_created(#{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_INFO("Session(~s) created, Session Info:~n~p~n", [ClientId, SessInfo]).

on_session_subscribed(#{clientid := ClientId}, Topic, SubOpts, _Env) ->
  ?LOG_INFO("Session(~s) subscribed ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]).

on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, _Env) ->
  ?LOG_INFO("Session(~s) unsubscribed ~s with opts: ~p~n", [ClientId, Topic, Opts]).

on_session_resumed(#{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_INFO("Session(~s) resumed, Session Info:~n~p~n", [ClientId, SessInfo]).

on_session_discarded(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_INFO("Session(~s) is discarded. Session Info: ~p~n", [ClientId, SessInfo]).

on_session_takenover(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_INFO("Session(~s) is takenover. Session Info: ~p~n", [ClientId, SessInfo]).

on_session_terminated(_ClientInfo = #{clientid := ClientId}, Reason, SessInfo, _Env) ->
  ?LOG_INFO("Session(~s) is terminated due to ~p~nSession Info: ~p~n",
    [ClientId, Reason, SessInfo]).

%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
  {ok, Message};

on_message_publish(Message, _Env) ->
  {ok, ClientId, Payload} = format_payload(Message),
  produce_kafka_payload(ClientId, Payload),
  {ok, Message}.

on_message_puback(_PacketId, #message{topic = _Topic} = Message, PubRes, _Env) ->
  NewRC = case PubRes of
            %% Demo: some service do not want to expose the error code (129) to client;
            %% so here it remap 129 to 128
            129 -> 128;
            _ ->
              PubRes
          end,
  ?LOG_INFO("Puback ~p RC: ~p~n",
    [emqx_message:to_map(Message), NewRC]),
  {ok, NewRC}.

on_message_dropped(#message{topic = <<"$SYS/", _/binary>>}, _By, _Reason, _Env) ->
  ok;
on_message_dropped(Message, _By = #{node := Node}, Reason, _Env) ->
  ?LOG_INFO("Message dropped by node ~p due to ~p:~n~p~n",
    [Node, Reason, emqx_message:to_map(Message)]).

on_message_delivered(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
  ?LOG_INFO("Message delivered to client(~s):~n~p~n",
    [ClientId, emqx_message:to_map(Message)]),
  Topic = Message#message.topic,
  Payload = transform_payload(Message#message.payload),
  Qos = Message#message.qos,
  From = Message#message.from,
  Timestamp = Message#message.timestamp,
  Content = [
    {action, <<"message_delivered">>},
    {from, From},
    {to, ClientId},
    {topic, Topic},
    {payload, Payload},
    {qos, Qos},
    {cluster_node, node()},
    {ts, Timestamp}
  ],
  produce_kafka_payload(ClientId, Content),
  {ok, Message}.

on_message_acked(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
  ?LOG_INFO("Message acked by client(~s):~n~p~n",
    [ClientId, emqx_message:to_map(Message)]),
  Topic = Message#message.topic,
  Payload = transform_payload(Message#message.payload),
  Qos = Message#message.qos,
  From = Message#message.from,
  Timestamp = Message#message.timestamp,
  Content = [
    {action, <<"message_acked">>},
    {from, From},
    {to, ClientId},
    {topic, Topic},
    {payload, Payload},
    {qos, Qos},
    {cluster_node, node()},
    {ts, Timestamp}
  ],
  produce_kafka_payload(ClientId, Content).


kafka_init(_Env) ->
  ?LOG_INFO("Start to init emqx plugin kafka..... ~n"),
  %% 使用硬编码值替换配置文件的调用
  AddressList = [{"localhost", 9092}],
  ?LOG_INFO("[KAFKA PLUGIN]KafkaAddressList = ~p~n", [AddressList]),
  KafkaConfig = [
    {reconnect_cool_down_seconds, 10},
    {query_api_versions, true}
  ],
  ?LOG_INFO("[KAFKA PLUGIN]KafkaConfig = ~p~n", [KafkaConfig]),
  KafkaTopic = <<"emqx-topic">>,
  ?LOG_INFO("[KAFKA PLUGIN]KafkaTopic = ~s~n", [KafkaTopic]),
  {ok, _} = application:ensure_all_started(brod),
  ok = brod:start_client(AddressList, emqx_repost_worker, KafkaConfig),
  ok = brod:start_producer(emqx_repost_worker, KafkaTopic, []),
  ?LOG_INFO("Init emqx plugin kafka successfully.....~n"),
  ok.

get_kafka_topic() ->
  <<"emqx-topic">>.

need_base64() ->
  false.

transform_payload(Payload) ->
  NeedBase64 = need_base64(),
  if
    NeedBase64 == true ->
      Content = list_to_binary(base64:encode_to_string(Payload));
    NeedBase64 == false ->
      Content = Payload
  end,
  Content.


format_payload(Message) ->
  Username = emqx_message:get_header(username, Message),
  Topic = Message#message.topic,
  % ?LOG_INFO("[KAFKA PLUGIN]Tail= ~s , RawType= ~s~n",[Tail,RawType]),
  ClientId = Message#message.from,
  Content = transform_payload(Message#message.payload),
  Payload = [{action, message_publish},
    {device_id, ClientId},
    {username, Username},
    {topic, Topic},
    {payload, Content},
    {ts, Message#message.timestamp}],

  {ok, ClientId, Payload}.


%% Called when the plugin application stop
unload() ->
  unhook('client.connect', {?MODULE, on_client_connect}),
  unhook('client.connack', {?MODULE, on_client_connack}),
  unhook('client.connected', {?MODULE, on_client_connected}),
  unhook('client.disconnected', {?MODULE, on_client_disconnected}),
  unhook('client.authenticate', {?MODULE, on_client_authenticate}),
  unhook('client.authorize', {?MODULE, on_client_authorize}),
  unhook('client.subscribe', {?MODULE, on_client_subscribe}),
  unhook('client.unsubscribe', {?MODULE, on_client_unsubscribe}),
  unhook('session.created', {?MODULE, on_session_created}),
  unhook('session.subscribed', {?MODULE, on_session_subscribed}),
  unhook('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
  unhook('session.resumed', {?MODULE, on_session_resumed}),
  unhook('session.discarded', {?MODULE, on_session_discarded}),
  unhook('session.takenover', {?MODULE, on_session_takenover}),
  unhook('session.terminated', {?MODULE, on_session_terminated}),
  unhook('message.publish', {?MODULE, on_message_publish}),
  unhook('message.puback', {?MODULE, on_message_puback}),
  unhook('message.delivered', {?MODULE, on_message_delivered}),
  unhook('message.acked', {?MODULE, on_message_acked}),
  unhook('message.dropped', {?MODULE, on_message_dropped}).

hook(HookPoint, MFA) ->
  %% use highest hook priority so this module's callbacks
  %% are evaluated before the default hooks in EMQX
  emqx_hooks:add(HookPoint, MFA, _Property = ?HP_HIGHEST).

unhook(HookPoint, MFA) ->
  emqx_hooks:del(HookPoint, MFA).

produce_kafka_payload(Key, Message) ->
  Topic = get_kafka_topic(),
  {ok, MessageBody} = emqx_utils_json:safe_encode(Message),
  ?LOG_INFO("[PLUGIN]Message = ~s~n", [MessageBody]),
  Payload = iolist_to_binary(MessageBody),
  brod:produce_cb(emqx_repost_worker, Topic, hash, Key, Payload, fun(_, _) -> ok end),
  ok.

ntoa({0, 0, 0, 0, 0, 16#ffff, AB, CD}) ->
  inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256});
ntoa(IP) ->
  inet_parse:ntoa(IP).
now_mill_secs({MegaSecs, Secs, _MicroSecs}) ->
  MegaSecs * 1000000000 + Secs * 1000 + _MicroSecs.

