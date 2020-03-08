package io.datajuice.nifi.processors;

import org.apache.nifi.processor.Relationship;

public class Relationships {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Avro content that converted successfully").build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure").description("Avro content that failed to convert")
            .build();

}
