package com.monetate.koupler;

import java.io.IOException;
import java.io.OutputStream;
import java.io.File;
import java.net.Socket;
import java.util.Arrays;
import java.util.Scanner;

import org.junit.Test;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpKouplerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TcpKouplerTest.class);
    private static final int TOTAL_LINES = 3;

    @Test
    public void test() throws IOException, InterruptedException {
        MockKinesisEventProducer mockProducer = new MockKinesisEventProducer();
        TcpKoupler server = new TcpKoupler(mockProducer, 4242, 20);
        Thread serverThread = new Thread(server);
        serverThread.start();
        Thread.sleep(1000);
        
        Socket socket = new Socket("localhost", 4242);
        OutputStream stream = socket.getOutputStream();
        for (int i = 0; i < TOTAL_LINES; i++) {
            sendEvent(stream, i);
        }
        
        LOGGER.debug("Hang tight -- waiting for TCP packets");
        mockProducer.waitFor(TOTAL_LINES);
        socket.close();
    }

    public void sendEvent(OutputStream stream, int x) throws IOException {
        String offer = String.format("foo,%s\n", x);
        LOGGER.info("Sending [{}]", offer);
        stream.write(offer.getBytes());
    }

    public void sendEvent(OutputStream stream, String event) throws IOException {
        LOGGER.info("Sending [{}]", event);
        stream.write(String.format("%s\n", event).getBytes());
    }

    @Test
    public void test_unicode() throws IOException, InterruptedException {
        MockKinesisEventProducer mockProducer = new MockKinesisEventProducer();
        TcpKoupler server = new TcpKoupler(mockProducer, 4242, 20);
        Thread serverThread = new Thread(server);
        serverThread.start();
        Thread.sleep(1000);

        File unicode_file = new File("src/test/resources/sample.txt");
        Scanner sc = new Scanner(unicode_file);

        Socket socket = new Socket("localhost", 4242);
        OutputStream stream = socket.getOutputStream();

        String event = sc.nextLine();
        sendEvent(stream, event);

        mockProducer.waitFor(1);
        socket.close();

        Assert.assertTrue(Arrays.equals(event.getBytes(), mockProducer.event.getBytes()));
    }
}
