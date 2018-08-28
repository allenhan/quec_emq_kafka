PROJECT = quec_emq_kafka
PROJECT_DESCRIPTION = EMQ Kafka Plugin
PROJECT_VERSION = 2.3.11

DEPS = jsx clique ekaf
dep_jsx    = git https://github.com/talentdeficit/jsx v2.8.3
dep_clique = git https://github.com/emqtt/clique v0.3.10
dep_ekaf   = git https://github.com/helpshift/ekaf master

BUILD_DEPS = emqttd cuttlefish
dep_emqttd = git https://github.com/emqtt/emqttd master
dep_cuttlefish = git https://github.com/emqtt/cuttlefish v2.0.11

ERLC_OPTS += +debug_info
ERLC_OPTS += +'{parse_transform, lager_transform}'

NO_AUTOPATCH = cuttlefish
COVER = true

include erlang.mk

app:: rebar.config

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/quec_emq_kafka.conf -i priv/quec_emq_kafka.schema -d data
