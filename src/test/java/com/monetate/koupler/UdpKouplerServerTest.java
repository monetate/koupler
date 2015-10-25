package com.monetate.koupler;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UdpKouplerServerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(UdpKouplerServerTest.class);

    private static final int TOTAL_PACKETS = 2;

    @Test
    public void test() throws IOException, InterruptedException {
        MockKinesisEventProducer mockProducer = new MockKinesisEventProducer();
        UdpKoupler server = new UdpKoupler(mockProducer, 4242);
        
        Thread serverThread = new Thread(server);
        serverThread.start();

        DatagramSocket clientSocket = new DatagramSocket();
        InetAddress ipAddress = InetAddress.getByName("localhost");
        long start = System.currentTimeMillis();
        for (int i = 0; i < TOTAL_PACKETS; i++) {
            sendEvent(ipAddress, clientSocket, i);
            Thread.sleep(50);
        }
        long stop = System.currentTimeMillis();
        LOGGER.debug("Emitted [{}] packets in [{}] milliseconds.  Hang tight, testing for dropped packets in UDP server.", TOTAL_PACKETS, stop - start);
        Thread.sleep(1000); // wait 1 seconds
        assertEquals("Did not receive all UDP packets!", TOTAL_PACKETS, UdpKoupler.PACKETS.get());
    }

    public void sendEvent(InetAddress ipAddress, DatagramSocket socket, int x) throws IOException {
        String offer = String.format("foo,%s", x);
        LOGGER.info("Sending [{}]", offer);
        byte[] sendData = offer.getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, ipAddress, 4242);
        socket.send(sendPacket);
    }

}
