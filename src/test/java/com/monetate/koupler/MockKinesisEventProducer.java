package com.monetate.koupler;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicInteger;

import com.monetate.koupler.KinesisEventProducer;

public class MockKinesisEventProducer extends KinesisEventProducer {
    public AtomicInteger COUNT = new AtomicInteger();
    
    @Override
    public void sendLine(String line) throws UnsupportedEncodingException {
        COUNT.getAndIncrement();
    }
}
