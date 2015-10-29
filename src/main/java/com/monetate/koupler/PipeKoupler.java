package com.monetate.koupler;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeKoupler extends Koupler implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEventProducer.class);

    public PipeKoupler(KinesisEventProducer producer){
        super(producer, 1);
        LOGGER.info("Firing up pipe listener");
    }
    
    @Override
    public void run(){
        InputStreamReader streamReader = new InputStreamReader(System.in);
        BufferedReader bufferedReader = new BufferedReader(streamReader);
        this.getThreadPool().submit(new KouplerThread(bufferedReader));   
    }
}
