package com.monetate.koupler.format;

import org.apache.commons.cli.CommandLine;

/**
 * Created by brianoneill on 4/29/16.
 */
public class FormatFactory {
    public static Format getFormat(String format, CommandLine cmd){
        if (format.equals("split")){
            return new SplitFormat(cmd);
        } else if (format.equals("json")) {
            return new JsonFormat();
        } else {
            String msg = String.format("Incompatible format specified for event. [%s]", format);
            throw new RuntimeException(msg);
        }

    }
}
