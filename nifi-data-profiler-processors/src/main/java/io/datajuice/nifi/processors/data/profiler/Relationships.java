package io.datajuice.nifi.processors.data.profiler;

import org.apache.nifi.processor.Relationship;

public class Relationships {

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Avro content that converted successfully").build();

    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure").description("Avro content that failed to convert")
            .build();

}
