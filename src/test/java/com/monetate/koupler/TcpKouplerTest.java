package com.monetate.koupler;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.monetate.koupler.TcpKoupler;

public class TcpKouplerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TcpKouplerTest.class);
    private static final int TOTAL_LINES = 3;

    @Test
    public void test() throws IOException, InterruptedException {
        MockKinesisEventProducer mockProducer = new MockKinesisEventProducer();
        TcpKoupler server = new TcpKoupler(mockProducer, 4242);
        Thread serverThread = new Thread(server);
        serverThread.start();
        Thread.sleep(1000);
        
        Socket socket = new Socket("localhost", 4242);
        OutputStream stream = socket.getOutputStream();
        for (int i = 0; i < TOTAL_LINES; i++) {
            sendEvent(stream, i);
        }
        
        LOGGER.debug("Hang tight -- waiting for TCP packets");
        Thread.sleep(1000);
        assertEquals("Did not receive all messages over TCP!", TOTAL_LINES, mockProducer.COUNT.get());
        socket.close();
    }

    public void sendEvent(OutputStream stream, int x) throws IOException {
        String offer = String.format("offer,1,999,2015-09-29 00:16:18,2,1443485683020,676098207,322877,%s\n", x);
        LOGGER.info("Sending [{}]", offer);
        stream.write(offer.getBytes());
    }

}
