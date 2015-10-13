package com.monetate.koupler;

import static spark.Spark.post;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.Request;
import spark.Response;
import spark.Route;

public class Koupler implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Koupler.class);

    public static void main(String[] args) throws Exception {
        Thread restServer = new Thread(new Koupler());
        restServer.start();
        PipeKoupler.main(args);
    }

    @Override
    public void run() {
        // TODO: Should move to JDK 8, so we can use SparkJava 2 (and lambdas!)
        post(new Route("/admin") {
            @Override
            public Object handle(Request request, Response response) {                
                String action = request.body();
                LOGGER.debug("Admin action received [{}]", action);
                if (action.equals("pause"))
                    KinesisEventProducer.PAUSED = true;
                else if (action.equals("resume"))
                    KinesisEventProducer.PAUSED = false;
                else if (action.equals("verbose"))
                    KinesisEventProducer.VERBOSE = true;
                else if (action.equals("quiet"))
                    KinesisEventProducer.VERBOSE = false;
                return KinesisEventProducer.METRICS.toString();
            }
        });
    }
}
