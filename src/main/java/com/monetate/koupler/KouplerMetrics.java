package com.monetate.koupler;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class KouplerMetrics implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KouplerMetrics.class);

    private final Meter queuedMeter;
    private final Meter completedMeter;
    private final Histogram bytesHistogram;
    private Dimension hostDimension = null;
    private KinesisEventProducer producer;
    public static final boolean RUNNING = true;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private String appName;
    private AmazonCloudWatchClient cloudWatch;
    private MetricRegistry metricsRegistry = new MetricRegistry();

    public KouplerMetrics(KinesisEventProducer producer, String appName) {
        this.producer = producer;
        this.appName = appName;
        queuedMeter = metricsRegistry.meter(MetricRegistry.name(KinesisEventProducer.class, "queued"));
        completedMeter = metricsRegistry.meter(MetricRegistry.name(KinesisEventProducer.class, "completed"));
        bytesHistogram = metricsRegistry.histogram(MetricRegistry.name(KinesisEventProducer.class, "bytes"));
        try {
            hostDimension = new Dimension().withName("Host").withValue(InetAddress.getLocalHost().toString());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public KouplerMetrics(KinesisEventProducer producer, KinesisProducerConfiguration config, String appName) {
        this(producer, appName);
        cloudWatch = new AmazonCloudWatchClient();
        Region region = Region.getRegion(Regions.fromName(config.getRegion()));
        cloudWatch.setRegion(region);
    }

    public void queueEvent(int numberOfBytes) {
        queuedMeter.mark();
        bytesHistogram.update(numberOfBytes);
    }

    public void ackEvent() {
        completedMeter.mark();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Queued: events / second = [").append(queuedMeter.getMeanRate()).append("]\n");
        sb.append("Completed: events / second = [").append(completedMeter.getMeanRate()).append("]\n");
        sb.append("Sizing: bytes / event = [").append(bytesHistogram.getSnapshot().getMean()).append("]\n");
        return sb.toString();
    }

    public double getEventsPerSecond() {
        return queuedMeter.getMeanRate();
    }

    public void start(int periodicityInSeconds) {
        scheduler.scheduleWithFixedDelay(this, 0, periodicityInSeconds, SECONDS);
    }

    public void run() {
        try {
            MetricDatum bytesPerEvent = new MetricDatum()
                    .withDimensions(hostDimension)
                    .withMetricName("BytesPerEvent")
                    .withValue(bytesHistogram.getSnapshot().getMean());

            MetricDatum kplEventQueueCount = new MetricDatum()
                    .withDimensions(hostDimension)
                    .withMetricName("KplEventQueueCount")
                    .withValue(producer.getKplQueueSize() * 1.0);

            MetricDatum internalEventQueueCount = new MetricDatum()
                    .withDimensions(hostDimension)
                    .withMetricName("InternalEventQueueCount")
                    .withValue(producer.getInternalQueueSize() * 1.0);

            
            MetricDatum queuedEventsPerSecond = new MetricDatum()
                    .withDimensions(hostDimension)
                    .withMetricName("QueuedEventsPerSecond")
                    .withValue(queuedMeter.getMeanRate());

            MetricDatum completedEventsPerSecond = new MetricDatum()
                    .withDimensions(hostDimension)
                    .withMetricName("CompletedEventsPerSecond")
                    .withValue(completedMeter.getMeanRate());

            cloudWatch.putMetricData(new PutMetricDataRequest().withMetricData(bytesPerEvent).withNamespace(appName));
            cloudWatch.putMetricData(new PutMetricDataRequest().withMetricData(kplEventQueueCount).withNamespace(appName));
            cloudWatch.putMetricData(new PutMetricDataRequest().withMetricData(internalEventQueueCount).withNamespace(appName));
            cloudWatch.putMetricData(new PutMetricDataRequest().withMetricData(queuedEventsPerSecond).withNamespace(appName));
            cloudWatch.putMetricData(new PutMetricDataRequest().withMetricData(completedEventsPerSecond).withNamespace(appName));
            
            LOGGER.debug("Published metrics to CloudWatch [{}].", this.toString());

        } catch (Exception e) {
            LOGGER.error("Problem posting metrics to cloudwatch.", e);
        }
    }
}
