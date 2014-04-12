twitter-tf-idf
==============

### Instalation and running

First, configure the Storm cluster by editing the file `${STORM_HOME}/conf/storm.yaml`:

	storm.zookeeper.servers:
	- "127.0.0.1"

	# the directory where Storm will keep temporary files, must exist
	storm.local.dir: "/var/storm"

	nimbus.host: "127.0.0.1"

	supervisor.slots.ports:
	- 6700

	drpc.servers:
	- "127.0.0.1"

	# jetty uses the default 8080, so change StormUI port to 8081
	ui.port: 8081

