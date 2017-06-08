package com.monetate.koupler;

import static spark.Spark.post;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpKoupler extends Koupler implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpKoupler.class);

    public HttpKoupler(KinesisEventProducer producer, int port, int threadPoolSize) {
        super(producer, threadPoolSize);
        LOGGER.info("Firing up HTTP listener on [{}]", port);
    }
    
    @Override
    public void run() {
        post("/event", (request, response) -> {
                String event = request.body();
                producer.queueEvent(event);
                return "ACK\n";
        });
    }
}
