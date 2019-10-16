package com.monetate.koupler;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.monetate.koupler.format.Format;
import com.monetate.koupler.format.FormatFactory;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class KinesisEventProducer implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEventProducer.class);
    public static final int DEFAULT_BACKOFF = 100; // (in ms)
    public static final int MAX_BACKOFF = 16000;
    private static int backOff = DEFAULT_BACKOFF;
    private static final Random RANDOM = new Random();
    public static boolean RUNNING = true;

    private KouplerMetrics metrics;
    private KinesisProducer producer;
    private String streamName;
    private Format format;
    private int throttleQueueSize;
    private String delimiter;
    private BlockingQueue<String> queue; 

    public KinesisEventProducer(int throttleQueueSize) {
        this.throttleQueueSize = throttleQueueSize;
        queue = new ArrayBlockingQueue<String>(throttleQueueSize);
    }

    public KinesisEventProducer(String format, CommandLine cmd,
                                String propertiesFile, String streamName,
                                int throttleQueueSize, String appName) {
        this(throttleQueueSize);
        KinesisProducerConfiguration config = KinesisProducerConfiguration.fromPropertiesFile(propertiesFile);
        this.streamName = streamName;
        this.producer = new KinesisProducer(config);
        this.metrics = new KouplerMetrics(this, config, appName);
        this.format = FormatFactory.getFormat(format, cmd);
    }

    public void queueEvent(String event) throws EventQueueFullException {
        if (queue.remainingCapacity() == 0){
            throw new EventQueueFullException(queue.size());
        }        
        this.queue.add(event);
    }
    
    public int getInternalQueueSize(){
        return this.queue.size();
    }

    public int getKplQueueSize(){
        return this.producer.getOutstandingRecordsCount();
    }

    public void clearQueue(){
        this.queue.clear();
    }
    
    public void startMetrics(){
        this.metrics.start(60);
    }

    public String getPartitionKey(String event) {
        try {
            return format.getPartitionKey(event);
        } catch (Exception e) {
            LOGGER.warn("Received event from which we could NOT extract partition key: " + event, e);
            return null;
        }
    }

    public String getData(String event) {
        try {
            return format.getData(event);
        } catch (Exception e) {
            LOGGER.warn("Received event from which we could NOT extractdata.", e);
            return null;
        }
    }
    
    /**
     * When run as a thread, this will use the buffered reader with which this
     * object was constructed.
     */
    @Override
    public void run() {
        while (RUNNING) {
            try {
                if (producer != null && producer.getOutstandingRecordsCount() > throttleQueueSize) {
                    LOGGER.warn("Throttling ingest, waiting [{}] milliseconds, for KPL to put [{}] records.",
                            backOff, this.producer.getOutstandingRecordsCount());
                    Thread.sleep(backOff);
                    backOff = Math.min(MAX_BACKOFF, backOff * 2) + RANDOM.nextInt(100);
                } else {
                    String event = queue.take();
                    send(event);
                    backOff = DEFAULT_BACKOFF;
                }
            } catch (Exception e) {
                LOGGER.error("Error while processing event queue.", e);
            }
        }
    }

    public void send(String event) throws UnsupportedEncodingException {
        byte[] bytes = event.getBytes("UTF-8");
        this.metrics.queueEvent(bytes.length);
        ByteBuffer data = ByteBuffer.wrap(bytes);
        String partitionKey = getPartitionKey(event);
        if (partitionKey != null) {
            ListenableFuture<UserRecordResult> f = producer.addUserRecord(streamName, partitionKey, data);
            Futures.addCallback(f, new FutureCallback<UserRecordResult>() {
                @Override
                public void onFailure(Throwable t) {
                    if (t instanceof UserRecordFailedException) {
                        Attempt last = Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
                        LOGGER.error(String.format("Record failed to put - %s : %s", last.getErrorCode(), last.getErrorMessage()));
                    }
                    LOGGER.error("Exception during put", t);
                }

                @Override
                public void onSuccess(UserRecordResult result) {
                    metrics.ackEvent();
                }
            });
        }
    }
}
