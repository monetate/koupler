package com.monetate.koupler;

import java.util.concurrent.atomic.AtomicInteger;

public class MockKinesisEventProducer extends KinesisEventProducer {
    public MockKinesisEventProducer() {
        super(10000);
    }

    public AtomicInteger COUNT = new AtomicInteger();
    
    @Override
    public void queueEvent(String event) {
        COUNT.getAndIncrement();
    }
}
