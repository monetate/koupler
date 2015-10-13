package com.monetate.koupler;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class KouplerMetrics implements Runnable {
	private final Meter queuedMeter;
	private final Meter completedMeter;
	private final Histogram bytesHistogram;

	private AmazonCloudWatchClient cloudWatch;
	private MetricRegistry metricsRegistry = new MetricRegistry();

	public KouplerMetrics() {
		queuedMeter = metricsRegistry.meter(MetricRegistry.name(KinesisEventProducer.class, "queued"));
		completedMeter = metricsRegistry.meter(MetricRegistry.name(KinesisEventProducer.class, "completed"));
		bytesHistogram = metricsRegistry.histogram(MetricRegistry.name(KinesisEventProducer.class, "bytes"));
	}

	public KouplerMetrics(KinesisProducerConfiguration config) {
		this();
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

	public double getEventsPerSecond(){
		return queuedMeter.getMeanRate();
	}
	
	public void run(){
		//logStatus();
	}
}
