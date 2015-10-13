package com.monetate.koupler;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.monetate.koupler.UdpKoupler;

public class UdpCouplerServerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(UdpCouplerServerTest.class);

    private static final int TOTAL_PACKETS = 2;

    @Test
    public void testLineSend() throws IOException, InterruptedException {
        PipedInputStream is = new PipedInputStream();
        PipedOutputStream out = new PipedOutputStream(is);
        UdpKoupler server = new UdpKoupler(out);
        Thread serverThread = new Thread(server);
        serverThread.start();

        DatagramSocket clientSocket = new DatagramSocket();
        InetAddress ipAddress = InetAddress.getByName("localhost");
        long start = System.currentTimeMillis();
        for (int i = 0; i < TOTAL_PACKETS; i++) {
            sendLine(ipAddress, clientSocket, i);
            Thread.sleep(50);
        }
        long stop = System.currentTimeMillis();
        LOGGER.debug("Emitted [{}] packets in [{}] milliseconds.  Hang tight, testing for dropped packets in UDP server.", TOTAL_PACKETS, stop - start);
        Thread.sleep(1000); // wait 1 seconds
        assertEquals("Did not receive all UDP packets!", TOTAL_PACKETS, UdpKoupler.PACKETS.get());
    }

    public void sendLine(InetAddress ipAddress, DatagramSocket socket, int x) throws IOException {
        String offer = String.format("offer,1,999,2015-09-29 00:16:18,2,1443485683020,676098207,322877,%s", x);
        LOGGER.info("Sending [{}]", offer);

        byte[] sendData = offer.getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, ipAddress, 8052);
        socket.send(sendPacket);
    }

}
