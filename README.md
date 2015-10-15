Koupler
=====================================

This project provides TCP, UDP and Pipe interaces for Amazon's Kinesis.  Underneath the covers, it uses
the Kinesis Producer Library (KPL).

Building
--------
```bash
   mvn -Dmaven.test.skip=true clean assembly:assembly
```

Usage
-------
After a successful build, simply run the following to get usage information:
```bash
  $./koupler.sh 
```

You should see the following:
```bash
   $ ./koupler.sh
   Must specify either: udp, tcp or pipe
   Must specify stream name.
   usage: java -jar koupler*.jar
    -delimiter <arg>          delimiter between fields (default: ',')
    -paritionKeyField <arg>   field containing partition key (default: 0)
    -pipe                     pipe mode
    -port <arg>               listening port (default: 4242)
    -propertiesFile <arg>     kpl properties file (default: ./conf/kpl.properties)
    -streamName <arg>         kinesis stream name
    -tcp                      tcp mode
    -udp                      udp mode
```

The Consumer
-----

To kick the tires a bit, you can start the built-in consumer.  The built-in consumer will output messages from the stream to the console.
 
```bash
   $ ./koupler.sh -consumer -streamName  boneill-dev-test
   [INFO] 2015-10-14 23:36:43,254 producer.KinesisProducerConfiguration.fromPropertiesFile - Attempting to load config from file ./conf/kpl.properties
   [2015-10-14 23:36:43.583341] [0x00007fff7120e000] [info] [metrics_manager.h:148] Uploading metrics to monitoring.us-east-1.amazonaws.com:443
   [INFO] 2015-10-14 23:36:43,915 producer.KinesisProducerConfiguration.fromPropertiesFile - Attempting to load config from file ./conf/kpl.properties
   ...
   INFO: Initializing shard shardId-000000000000 with TRIM_HORIZON
```

TCP
-----
Next, fire up the TCP server and throw some data at it!

```bash
   $ ./koupler.sh -tcp -streamName boneill-dev-test
```

You can then fire events at the TCP server directly.  Each line represents an event, which will be sent through Kinesis using KPL.
```bash
   $ telnet localhost 4242
   Trying ::1...
   Connected to localhost.
   Escape character is '^]'.
   lisa
   collin
owen
```




