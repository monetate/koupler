package com.monetate.koupler.format;

import org.apache.commons.cli.CommandLine;

/**
 * Created by brianoneill on 4/29/16.
 */
public class SplitFormat implements Format {
    private int partitionKeyField = 0;
    private String delimiter = ",";

    /**
     * Constructor for Split format
     */
    public SplitFormat(CommandLine cmd){
        if (cmd.hasOption("delimiter")) {
            delimiter = cmd.getOptionValue("delimiter");
        }

        if (cmd.hasOption("partitionKeyField")) {
            partitionKeyField = Integer.parseInt(cmd.getOptionValue("partitionKeyField"));
        }
    }

    @Override
    public String getPartitionKey(String event) {
        return event.split(this.delimiter)[partitionKeyField];
    }

    @Override
    public String getData(String event) {
        return event;
    }
}