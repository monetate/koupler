package com.monetate.koupler.format;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;

/**
 * Created by jpalladino on 8/20/16.
 */
public class JsonFormat implements Format {

    @Override
    public String getPartitionKey(String event) {
        Object jsonEvent = Configuration.defaultConfiguration().jsonProvider().parse(event);
        return JsonPath.read(jsonEvent, "$.partitionKey");
    }

    @Override
    public String getData(String event) { return event; }
}
