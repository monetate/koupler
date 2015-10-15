Koupler
=====================================

This project provides TCP, UDP and Pipe interaces for Kinesis.

Building
--------
```bash
   mvn -Dmaven.test.skip=true clean assembly:assembly
```

Usage
-------
After a successful compile, simply run:
```bash
  $./koupler.sh 
```

Here is the usage information:
```bash
  $./koupler.sh 
```


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


