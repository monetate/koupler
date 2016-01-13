package com.monetate.koupler;

import static org.junit.Assert.*;

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
    public void testThreadRecovery() throws InterruptedException, ExecutionException{
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
        assertEquals("Koupler did no recover: did not receive an event after initial socket drop.", 1, numEvents);
        LOGGER.debug("Received [{}] events from socket [{}]", future.get());        
    }

}
