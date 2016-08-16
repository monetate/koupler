package com.monetate.koupler;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

public class KinesisEventConsumer implements IRecordProcessorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEventConsumer.class);
    private Worker.Builder builder;

    public KinesisEventConsumer(String propertiesFile, String streamName, String appName, String initialPosition) {
        KinesisProducerConfiguration config = KinesisProducerConfiguration.fromPropertiesFile(propertiesFile);

        InitialPositionInStream position = InitialPositionInStream.valueOf(initialPosition);
        
        KinesisClientLibConfiguration clientConfig = new KinesisClientLibConfiguration(appName, streamName,
                new DefaultAWSCredentialsProviderChain(), appName)
                        .withRegionName(config.getRegion())
                        .withInitialPositionInStream(position);
        
        this.builder = new Worker.Builder().recordProcessorFactory(this).config(clientConfig);
    }
    
    public void start(){
        this.builder.build().run();
    }

    @Override
    public IRecordProcessor createProcessor() {
        LOGGER.debug("Creating recordProcessor.");
        return new IRecordProcessor() {
            @Override
            public void initialize(String shardId) {}

            @Override
            public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
                for (Record record : records){ 
                    byte[] bytes = new byte[record.getData().remaining()]; 
                    record.getData().get(bytes);
                    String data = new String(bytes);
                    LOGGER.debug("Received [{}]", data);
                }
            }

            @Override
            public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
            }
        };

    }
}
