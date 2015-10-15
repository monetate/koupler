package com.monetate.koupler;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.junit.Test;

public class PipeKouplerTest {
    private static final int TOTAL_LINES = 5;

	@Test
	public void test() throws IOException, InterruptedException {
        MockKinesisEventProducer mockProducer = new MockKinesisEventProducer();
        PipedOutputStream outputStream = new PipedOutputStream();
        PipedInputStream inputStream = new PipedInputStream(outputStream);
        System.setIn(inputStream);

        PipeKoupler koupler = new PipeKoupler(mockProducer);
        Thread thread = new Thread(koupler);
        thread.start();
        
        for (int i=0; i < TOTAL_LINES; i++){
            this.sendEvent(outputStream);
        }        
        Thread.sleep(500);
        assertEquals("Did not queue all records!", TOTAL_LINES, mockProducer.COUNT.get());     
	}
	
    public void sendEvent(PipedOutputStream out) throws IOException {
        String offer = "offer,1,999,2015-09-29 00:16:18,2,1443485683020,676098207,322877,1\n";
        byte[] sendData = offer.getBytes();
        out.write(sendData);
    }

}
