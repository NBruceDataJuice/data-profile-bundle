package io.datajuice.nifi.processors.data.profiler;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestFlattenAvro {

    TestRunner testRunner;

    @Before
    public void init() {
        testRunner =  TestRunners.newTestRunner(FlattenAvro.class);
    }

    @Test
    public void testProcessor() throws IOException {
        // Manual test
        FileInputStream avroFile = new FileInputStream("src/test/resources/avroDir/mockAvro.avro");
        testRunner.enqueue(avroFile);
        testRunner.run();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(Relationships.SUCCESS);
        MockFlowFile result = results.get(0);

        InputStream targetStream = new ByteArrayInputStream(result.toByteArray());
        DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>();
        DataFileStream<GenericData.Record> stream = new DataFileStream<>(targetStream, datumReader);

        for(GenericRecord record: stream){
            System.out.println(record);
        }

    }
}
