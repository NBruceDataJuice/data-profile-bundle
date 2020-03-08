package io.datajuice.nifi.processors.utils;

import io.datajuice.nifi.processors.descriptor.BooleanColumn;
import io.datajuice.nifi.processors.descriptor.NumericColumn;
import io.datajuice.nifi.processors.descriptor.StringColumn;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class ProfileManager {

    // TODO Optimizations
    // Slower implementation that treats Avro file like a column store. Maybe the next iteration has a conversion step
    // to turn Avro into Parquet or something. The key point here is that we don't want to keep more data in memory
    // than necessary. So, we are willing to sacrifice compute time for a smaller memory footprint. With that being said
    // this footprint could still get rather large. It is all really about the size of the file. It is important to
    // accurately convey this to the users

    // TODO We don't need to flatten a file before profiling it. I started down this path bc I was trying to conceptualize
    //  exactly how this would work. Next step needs to include a recursive function to apply the logic to an Avro file
    //  as is rests without flattening it

    public static FlowFile profileManager(ProcessSession session, FlowFile flowFile) {

        Schema incomingSchema = getIncomingSchema(session, flowFile);
        Map<String, List<String>> datatypeMap = createDatatypeMapping(incomingSchema);
        final Schema schema = ReflectData.get().getSchema(io.datajuice.nifi.processors.descriptor.Profile.class);
        FlowFile newFlowFile = session.create(flowFile);

        try {
            session.write(newFlowFile, (IgnoredInputStream, OutputStream) -> {

                DatumWriter<io.datajuice.nifi.processors.descriptor.Profile> datumWriter = new ReflectDatumWriter<>(schema);
                DataFileWriter<io.datajuice.nifi.processors.descriptor.Profile> dataFileWriter = new DataFileWriter<>(datumWriter);
                dataFileWriter.setCodec(CodecFactory.snappyCodec());
                dataFileWriter.create(schema, OutputStream);

                // We are going to go through datatypeMap and for each column present, we are going to get the descriptive stats
                // for that column, 1 column at a time, and add it to the statsMap
                for (Map.Entry<String, List<String>> entry : datatypeMap.entrySet()) {
                    for (String column : entry.getValue()) {
                        profileColumnToAvro(session, flowFile, entry, column, dataFileWriter);
                    }
                }

                dataFileWriter.close();
            });
        } catch (Exception e){
            session.remove(newFlowFile);
            session.putAttribute(flowFile, "Exception", e.toString());
        }
        return newFlowFile;
    }

    static void profileColumnToAvro(ProcessSession session, FlowFile flowFile, Map.Entry<String, List<String>> entry,
                          String column, DataFileWriter<io.datajuice.nifi.processors.descriptor.Profile> dataFileWriter){

        session.read(flowFile, inputStream -> {

            io.datajuice.nifi.processors.descriptor.Profile profile = null;
            DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>();
            DataFileStream<GenericData.Record> stream = new DataFileStream<>(inputStream, datumReader);
            switch (entry.getKey()) {
                case "number":
                    profile = new io.datajuice.nifi.processors.descriptor.Profile(column, new NumericColumn(column, stream));
                    break;

                case "boolean":
                    profile = new io.datajuice.nifi.processors.descriptor.Profile(column, new BooleanColumn(column, stream));
                    break;

                case "string":
                    profile = new io.datajuice.nifi.processors.descriptor.Profile(column, new StringColumn(column, stream));
                    break;

                default:
                    break;
            }

            if (profile != null) {
                dataFileWriter.append(profile);
            }

        });
    }

    static Map<String, List<String>> createDatatypeMapping(Schema outputSchema){
        Map<String, List<String>> datatypeMapping = new HashMap<>();
        for (Schema.Field field: outputSchema.getFields()) {
            switch(field.schema().getType().name().toLowerCase()){
                case "int":
                case "long":
                case "float":
                case "double":
                    if ( datatypeMapping.get("number") == null ){
                        List<String> numberColumns = new ArrayList();
                        datatypeMapping.put("number", numberColumns);
                    }
                    datatypeMapping.get("number").add(field.name());
                    break;

                case "string":
                    if ( datatypeMapping.get("string") == null ){
                        List<String> stringColumns = new ArrayList();
                        datatypeMapping.put("string", stringColumns);
                    }
                    datatypeMapping.get("string").add(field.name());
                    break;

                case "boolean":
                    if ( datatypeMapping.get("boolean") == null ){
                        List<String> booleanColumns = new ArrayList();
                        datatypeMapping.put("boolean", booleanColumns);
                    }
                    datatypeMapping.get("boolean").add(field.name());

                default:
                    break;
            }


        }

        return datatypeMapping;
    }

    static Schema getIncomingSchema(ProcessSession session, FlowFile flowFile){
        AtomicReference<Schema> schemaFromIncomingFile = new AtomicReference<>();
        session.read(flowFile, inputStream -> {
            DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>();
            DataFileStream<GenericData.Record> stream = new DataFileStream<>(inputStream, datumReader);
            schemaFromIncomingFile.set(stream.getSchema());
        });
        return schemaFromIncomingFile.get();
    }


}

