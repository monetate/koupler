package com.monetate.koupler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by aso on 10/1/15.
 */

public class UdpKoupler implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(UdpKoupler.class);
    protected DatagramSocket socket = null;
    private boolean running = true;

    private final int port = 8052;
    private final int BUF_SIZE = 8192;
    private byte[] buf = new byte[BUF_SIZE];
    private PrintWriter writer;
    public static final AtomicLong PACKETS = new AtomicLong(0);

    public UdpKoupler(OutputStream out) throws IOException {
        writer = new PrintWriter(out);
    }

    public void run() {
        if (socket != null && socket.isBound())
            socket.close();
        try {
            LOGGER.info("Creating listening socket on port [{}]", port);
            socket = new DatagramSocket(port);
            socket.setReceiveBufferSize(1024 * 1024 * 1024);
        } catch (java.net.SocketException e) {
            LOGGER.error("Error creating socket: ", e);
            running = false;
        }

        while (running) {
            try {
                DatagramPacket packet = new DatagramPacket(buf, BUF_SIZE);
                socket.receive(packet);
                PACKETS.getAndIncrement();

                byte[] received = new byte[packet.getLength()];
                System.arraycopy(buf, 0, received, 0, packet.getLength());
                String msg = new String(buf, 0, packet.getLength());
                LOGGER.debug("Received [{}] via UDP, [{}] total messages", msg, PACKETS.get());
                writer.write(msg);
            } catch (java.io.IOException e) {
                LOGGER.error("Error receiving packet: ", e);
                running = false;
            }
        }
        try {
            socket.close();
        } catch (Exception e) {
            LOGGER.error("Could not close socket.", e);
        }
        socket = null;
    }

    /**
     * In the UDP case, we start two threads. One reading the datagrams, and the
     * other processing them from the stream.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        LOGGER.info("Starting the Kinesis Fact Producer Destination Server");
        String streamName = args[0];
        String propertiesFile = args[1];
        PipedInputStream is = new PipedInputStream();
        PipedOutputStream out = new PipedOutputStream(is);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
        KinesisEventProducer factProducer = new KinesisEventProducer(propertiesFile, streamName);
        factProducer.setBufferedReader(bufferedReader);
        
        Thread adminThread = new Thread(new Koupler());
        adminThread.start();

        Thread producerThread = new Thread(factProducer);
        producerThread.start();

        Thread serverThread = new Thread(new UdpKoupler(out));
        serverThread.start();
        serverThread.join();
    }

}
