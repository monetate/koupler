package com.monetate.koupler;

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
            this.sendEvent(outputStream, i);
        }
        
        mockProducer.waitFor(TOTAL_LINES);
	}
	
    public void sendEvent(PipedOutputStream out, int x) throws IOException {
        String offer = String.format("foo,%s\n", x);
        byte[] sendData = offer.getBytes();
        out.write(sendData);
    }
    
}
