twitter-tf-idf
==============

### Running Storm cluster

Storm uses Zookeeper for coordinating the cluster. You can find installation guide for a standalone cluster here: `http://zookeeper.apache.org/doc/r3.3.3/zookeeperStarted.html#sc_InstallingSingleMode`.

After you've installed and run Zookeeper, download and install Storm from `http://storm.incubator.apache.org/downloads.html`.

First, configure the Storm cluster by editing the file `${STORM_HOME}/conf/storm.yaml`:

	storm.zookeeper.servers:
	- "127.0.0.1"

	# the directory where Storm will keep temporary files, must exist
	storm.local.dir: "/var/storm"

	nimbus.host: "127.0.0.1"

	# we need at least two workers, one for Twitter scraper and one for tf-idf topology
	supervisor.slots.ports:
	- 6700
	- 6701

	drpc.servers:
	- "127.0.0.1"

	# jetty uses the default 8080, so change StormUI port to 8081
	ui.port: 8081

Now you can run the Storm cluster executing the following commands in the console:

    storm nimbus
    storm supervisor
    storm drpc
    
Optionally, if you want to use StormUI (recommended), run `storm ui`.

If you didn't get any errors, you can double check if the cluster is running by navigating to `localhost:8081` in your web browser. You should see StormUI dashboard with info about the cluster.

### Starting MongoDB

Parts of the application use MongoDB, which you should install and then start by running `mongod`.

### Deploying topology to the cluster

Navigate to the `storm` subproject in your cloned repository. There, create file `src/main/resources/tf-idf.properties`, e.g.

    REDIS_HOST=localhost
    MONGO_HOST=localhost
    MONGO_PORT=27017

Execute:

    mvn package -DskipTests
    
As the topology is not yet deployed the tests would be failing preventing packaging of the project. Therefore we skip tests here.

Next, deploy the topology:

    storm jar target/twitter-tf-idf-storm-1.0.jar to.us.bachor.iosr.TfIdfRunner
    
Optionally, if you want to develop the project under Eclipse, run `mvn eclipse:eclipse`.

The deployed topology should be visible in StormUI.

### Deploying the web server

Navigate to the `web` subproject in your cloned repository. There, like before, create file `src/main/resources/tf-idf.properties`

Execute:

    mvn jetty:run
    
Jetty web server will be started on port 8080. To see the DRPC results for a word (e.g. "have"), navigate in your web browser to:

    localhost:8080/frequencies/have
   

