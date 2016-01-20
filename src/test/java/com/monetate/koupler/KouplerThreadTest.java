package com.monetate.koupler;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KouplerThreadTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(KouplerThreadTest.class);

    @Test
    public void testThreadRecovery() throws InterruptedException, ExecutionException {
        Koupler koupler = new TcpKoupler(new MockKinesisEventProducer(), 5000);
        String nullData = ""; // will cause a socket drop
        InputStream is = new ByteArrayInputStream(nullData.getBytes());
        BufferedReader data = new BufferedReader(new InputStreamReader(is));
        Future<Integer> future = koupler.getThreadPool().submit(koupler.new KouplerThread(data));
        LOGGER.debug("Received [{}] events from socket [{}]", future.get());

        String goodData = "valid line"; // will cause a socket drop
        is = new ByteArrayInputStream(goodData.getBytes());
        data = new BufferedReader(new InputStreamReader(is));
        future = koupler.getThreadPool().submit(koupler.new KouplerThread(data));
        int numEvents = future.get();
        assertEquals("Koupler did not recover: did not receive an event after initial socket drop.", 1, numEvents);
        LOGGER.debug("Received [{}] events from socket [{}]", future.get());
    }

    @Test
    public void testQueueFull() throws InterruptedException, ExecutionException {
        int throttleQueueSize = 10000;
        KinesisEventProducer producer = new KinesisEventProducer(throttleQueueSize);
        Koupler koupler = new TcpKoupler(producer, 5000);
        StringBuilder sb = new StringBuilder();
        // try to queue an extra 2
        for (int i = 0; i < throttleQueueSize + 2; i++) {
            sb.append("event1:data......................................................................\n");
        }
        InputStream is = new ByteArrayInputStream(sb.toString().getBytes());
        BufferedReader data = new BufferedReader(new InputStreamReader(is));
        Future<Integer> future = koupler.getThreadPool().submit(koupler.new KouplerThread(data));
        int numEventsProcessed = future.get();
        assertEquals("Somehow didn't process the number of events than the queue allows!", throttleQueueSize,
                numEventsProcessed);

        producer.clearQueue(); // Simulate recovery!

        sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append("event1:data......................................................................\n");
        }
        is = new ByteArrayInputStream(sb.toString().getBytes());
        data = new BufferedReader(new InputStreamReader(is));
        future = koupler.getThreadPool().submit(koupler.new KouplerThread(data));
        numEventsProcessed = future.get();
        assertEquals("Didn't recover.", 10, numEventsProcessed);
    }

}
