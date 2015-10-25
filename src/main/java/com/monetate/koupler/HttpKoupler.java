package com.monetate.koupler;

import static spark.Spark.post;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpKoupler extends Koupler implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpKoupler.class);

    public HttpKoupler(KinesisEventProducer producer, int port) {
        super(producer, 20);
        LOGGER.info("Firing up HTTP listener on [{}]", port);
    }
    
    @Override
    public void run() {
        // TODO: Should move to JDK 8, so we can use SparkJava 2 (and lambdas!)
        post("/event", (request, response) -> {
                String action = request.body();
                LOGGER.debug("Admin action received [{}]", action);
                return producer.toString();
        });
    }
}
