-module(kafka_emqx_plugin_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([ start/2
        , stop/1
        ]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = kafka_emqx_plugin_sup:start_link(),
    kafka_emqx_plugin:load(application:get_all_env()),

    emqx_ctl:register_command(kafka_emqx_plugin, {kafka_emqx_plugin_cli, cmd}),
    {ok, Sup}.

stop(_State) ->
    emqx_ctl:unregister_command(kafka_emqx_plugin),
    kafka_emqx_plugin:unload().
