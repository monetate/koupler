package com.monetate.koupler;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.monetate.koupler.KouplerMetrics;

public class KouplerMetricsTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(KouplerMetricsTest.class);
	private Random random = new Random();

	@Test
	public void testMetrics() throws InterruptedException {
        MockKinesisEventProducer mockProducer = new MockKinesisEventProducer();
		KouplerMetrics metrics = new KouplerMetrics(mockProducer, "mock-test");
		for (int i = 0; i < 1000; i++) {
			metrics.queueEvent(random.nextInt(100));
		}
		Thread.sleep(500);
		LOGGER.debug(metrics.toString());
		assertTrue("Did not process events per second properly.", metrics.getEventsPerSecond() > 10);
	}

}
