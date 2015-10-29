package com.monetate.koupler;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UDP Listener
 */
public class UdpKoupler extends Koupler implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(UdpKoupler.class);
    protected DatagramSocket socket = null;
    private static final int BUF_SIZE = 8192;
    private byte[] buf = new byte[BUF_SIZE];
    public static final AtomicLong PACKETS = new AtomicLong(0);

    public UdpKoupler(KinesisEventProducer producer, int port) throws SocketException {
        super(producer, 1);
        LOGGER.info("Firing up UDP listener on [{}]", port);
        socket = new DatagramSocket(port);
        socket.setReceiveBufferSize(1024 * 1024 * 1024);
    }

    public void run() {
        try {
            while (true) {
                DatagramPacket packet = new DatagramPacket(buf, BUF_SIZE);
                socket.receive(packet);
                PACKETS.getAndIncrement();
                byte[] received = new byte[packet.getLength()];
                System.arraycopy(buf, 0, received, 0, packet.getLength());
                String event = new String(buf, 0, packet.getLength()).trim();
                LOGGER.debug("Queueing event [{}]", event);
                producer.queueEvent(event);
            }
        } catch (Exception e){
            LOGGER.error("Problem with socket.", e);
            
        } finally {
            socket.close();
        }

    }
}
