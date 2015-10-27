Koupler
=====================================

This project provides TCP, HTTP, UDP and Pipe interaces for Amazon's Kinesis.  Underneath the covers, it uses
the [Kinesis Producer Library (KPL)](https://github.com/awslabs/amazon-kinesis-producer).  The daemon 
listens on TCP/UDP/HTTP, or takes input from a pipe.  Regardless of the mode, it handles the stream line-by-line, 
splitting the line based on the delimiter supplied, and then uses the specified field as the Kinesis partition key
and queues the message with KPL.

Koupler also tracks metrics using [Coda Hale's most excellent metrics library](https://dropwizard.github.io/metrics/3.1.0/).  Those metrics
are then published up to Amazon's cloudwatch, allowing you to see per host behavior and throughput information.  For more information, 
see the metrics section below.

Building
--------
Koupler uses [gradle](http://gradle.org/) as its build system.  To build kouple with gradle, run the following:
```bash
   gradle clean build copyRuntimeLibs
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
    -http                     http mode
    -udp                      udp mode
```

The parameters are fairly straight-forward, but be sure to have a look at ```conf/kpl.properties```.
Also, you can control logging levels by changing ```conf/log4j2.xml```.


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
Next, fire up the TCP server and throw some data at it!  The following is an example command-line.

```bash
   $ ./koupler.sh -tcp -streamName boneill-dev-test
```

You can sling data at the UDP listener with the following:

```bash
   $ telnet localhost 4242
   Trying ::1...
   Connected to localhost.
   Escape character is '^]'.
   lisa
   collin
   owen
```

And in the consumer you should see:
```bash
   [DEBUG] 2015-10-14 23:50:24,456 koupler.KinesisEventConsumer.processRecords - Recieved [lisa]
   [DEBUG] 2015-10-14 23:50:24,456 koupler.KinesisEventConsumer.processRecords - Recieved [collin]
   [DEBUG] 2015-10-14 23:50:24,456 koupler.KinesisEventConsumer.processRecords - Recieved [owen]
```

UDP
-----

Next, fire up the UDP server!  The following is an example command-line.

```bash
   $ ./koupler.sh -udp -streamName boneill-dev-test
```

You can sling data at the UDP listener with the following:

```bash
   $ nc -u localhost 4242
   murphy
   bailey
```

HTTP
-----

Next, fire up the HTTP server! The server takes a POST, and queues the body of the HTTP request.
 The following is an example command-line.

```bash
   $ ./koupler.sh -http -streamName boneill-dev-test
```

You can sling data at the UDP listener with the following:

```bash
   $ curl -d "drago" http://localhost:4567/event
   ACK
```

Pipe
-----

Finally, for those that like pipes, we have the always versatile pipe version:

```bash
   $ printf "hello\nworld\n" | ./koupler.sh -pipe -streamName boneill-dev-test
   [INFO] 2015-10-15 00:18:05,031 producer.KinesisProducerConfiguration.fromPropertiesFile - Attempting to load config from file ./conf/kpl.properties
   [INFO] 2015-10-15 00:18:05,058 producer.KinesisProducer.extractBinaries - Extracting binaries to /var/folders/2f/wqb5702967s58rtsgb5kzd940000gp/T/amazon-kinesis-producer-native-binaries
   [2015-10-15 00:18:05.360559] [0x00007fff7120e000] [info] [metrics_manager.h:148] Uploading metrics to monitoring.us-east-1.amazonaws.com:443
   [INFO] 2015-10-15 00:18:05,699 koupler.KinesisEventProducer.<init> - Firing up pipe listener
   [DEBUG] 2015-10-15 00:18:05,703 koupler.Koupler.call - Queueing event [hello]
   [DEBUG] 2015-10-15 00:18:05,703 koupler.Koupler.call - Queueing event [world]
```

Metrics
-----
Koupler keeps track of following metrics.  These metrics are available in CloudWatch under 'Custom Metrics', 

Metric | Description
-------|----------------
BytesPerEvent | Average bytes per event / message
CompletedEventsPerSecond | Events per second successfully ack'd by Kinesis
QueuedEventsPerSecond | Events per second queued with the Kinesis Producer Library (KPL)

