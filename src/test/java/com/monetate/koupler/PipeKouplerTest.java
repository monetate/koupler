package com.monetate.koupler;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.junit.Test;

public class PipeKouplerTest {
    private static final int TOTAL_LINES = 5;

	@Test
	public void testThroughput() throws IOException, InterruptedException {
        PipedInputStream is = new PipedInputStream(); 
        PipedOutputStream out = new PipedOutputStream(is);

        // Use this line for integration testing!
        // KinesisFactProducer factProducer = new KinesisFactProducer("conf/kpl.properties", "boneill-fact-dev");
        MockKinesisEventProducer mockProducer = new MockKinesisEventProducer();

        mockProducer.setBufferedReader(new BufferedReader(new InputStreamReader(is)));
        Thread runThread = new Thread(mockProducer);
        runThread.start();
        
        for (int i=0; i < TOTAL_LINES; i++){
            this.sendLine(out);
        }
        
        Thread.sleep(10);
        assertEquals("Did not queue all records!", TOTAL_LINES, mockProducer.COUNT.get());     
	}
	
    public void sendLine(PipedOutputStream out) throws IOException {
        String offer = "offer,1,999,2015-09-29 00:16:18,2,1443485683020,676098207,322877,1\n";
        byte[] sendData = offer.getBytes();
        out.write(sendData);
    }

}
