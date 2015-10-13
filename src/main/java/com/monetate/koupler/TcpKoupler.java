package com.monetate.koupler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpKoupler implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TcpKoupler.class);
    private KinesisEventProducer factProducer;

    public TcpKoupler(KinesisEventProducer factProducer) throws IOException {
        this.factProducer = factProducer;
    }

    @Override
    public void run() {
        LOGGER.debug("Firing up TCP server...");
        try {
            ServerSocket listener = new ServerSocket(4242);
            try {
                while (true) {
                    Socket socket = listener.accept();
                    LOGGER.info("Accepting new socket.");
                    try {
                        BufferedReader data = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        factProducer.publishEventsFrom(data);
                    } finally {
                        socket.close();
                    }
                }
            } finally {
                listener.close();
            }
        } catch (Exception e){
            LOGGER.error("Socket problems.", e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        LOGGER.info("Starting the Kinesis Fact Producer Destination TCP Server");
        String streamName = args[0];
        String propertiesFile = args[1];
        KinesisEventProducer factProducer = new KinesisEventProducer(propertiesFile, streamName);

        Thread adminThread = new Thread(new Koupler());
        adminThread.start();
        
        Thread serverThread = new Thread(new TcpKoupler(factProducer));
        serverThread.start();
        serverThread.join();
    }

}
