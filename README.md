Proof of Concept for Real-time Ingest
=====================================

This project generates an executable jar that is capable of taking input from STDIN or from a compressed log
retrieved from S3 and outputing records to a named Kinesis stream.

Building
--------

This module will stream data from log files stored in S3 to a kinesis stream.

To build:
```bash
   /build.sh
```

To build and copy artifact into S3:
```bash
   /build.sh install
```

Testing
-------

Ensure the right JDK is being used and then run:

`mvn test`

Using
-----

The executable is expecting a properties file to be located at `/mnt/deployment/real-time/kpl.properties`
This file should contain properties for a KPL app/producer.

[example](https://github.com/awslabs/amazon-kinesis-producer/blob/master/java/amazon-kinesis-producer-sample/default_config.properties)



To run:
```bash

    # Ensure JVM > 1.7 is being used
    # To play a log file out of S3 into Kinesis
    java -jar target/stream-tributary.jar test-stream log.monetate.net fact_page_view/2015/08/01/

    # To send messages from STDIN to the specified Kinesis stream
    # Ensure kpl.properties is located in same dir as jar
    seq 10 | /opt/jdk1.7.0_79/bin/java -jar /mnt/deployment/real-time/stream-tributary.jar test-stream
```

Logging
-------

Logs are currently configured to write out to the console as well as /var/log/monetate/stream-tributary.log

To override logging with a custom log4j config, specify the system property `-Dlog4j
.configurationFile=path/to/your/log4j2.xml`


