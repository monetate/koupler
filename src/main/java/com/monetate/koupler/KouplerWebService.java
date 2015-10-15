package com.monetate.koupler;

import static spark.Spark.post;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.Request;
import spark.Response;
import spark.Route;

public class KouplerWebService implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KouplerWebService.class);
    private KinesisEventProducer producer;

    public KouplerWebService(KinesisEventProducer producer){
        this.producer = producer;
    }
    
    @Override
    public void run() {
        // TODO: Should move to JDK 8, so we can use SparkJava 2 (and lambdas!)
        post(new Route("/admin") {
            @Override
            public Object handle(Request request, Response response) {                
                String action = request.body();
                LOGGER.debug("Admin action received [{}]", action);
                return producer.toString();
            }
        });
    }
}
