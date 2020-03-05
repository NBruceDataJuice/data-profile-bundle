package io.datajuice.nifi.processors.data.profiler;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.util.AvroFlattener;

import org.apache.nifi.processors.kite.AvroRecordConverter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.apache.gobblin.util.WriterUtils.getCodecFactory;

public class Util {

    static Schema flatten(InputStream inputStream, boolean flattenComplexTypes) throws IOException {
        DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>();
        DataFileStream<GenericData.Record> stream = new DataFileStream<>(inputStream, datumReader);
        Schema inputSchema = stream.getSchema();

        return flatten(inputSchema, flattenComplexTypes);
    }

    static Schema flatten(Schema inputSchema, boolean flattenComplexTypes){
        return new AvroFlattener().flatten(inputSchema,  flattenComplexTypes);
    }

    static Schema flattenExperimental(InputStream inputStream, boolean flattenComplexTypes, boolean flattenHierarchy) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException, IOException {
        DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>();
        DataFileStream<GenericData.Record> stream = new DataFileStream<>(inputStream, datumReader);
        Schema inputSchema = stream.getSchema();

        return flattenExperimental(inputSchema, flattenComplexTypes, flattenHierarchy);
    }

    static Schema flattenExperimental(Schema inputSchema, boolean flattenComplextTypes, boolean flattenHierarchy) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        AvroFlattener avroFlattener = new AvroFlattener();
        Class<?> clazz = avroFlattener.getClass();
        Method method = clazz.getDeclaredMethod("flatten", Schema.class, boolean.class, boolean.class);
        method.setAccessible(true);

        return (Schema) method.invoke(avroFlattener, inputSchema, flattenComplextTypes, flattenHierarchy);
    }

    static void convertAvroFile(InputStream inputStream, OutputStream outputStream) throws IOException {
        DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>();
        DataFileStream<GenericData.Record> stream = new DataFileStream<>(inputStream, datumReader);
        Schema inputSchema = stream.getSchema();

        Schema outputSchema = flatten(inputSchema, true);

        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(outputSchema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
        dataFileWriter.setCodec(CodecFactory.snappyCodec());
        dataFileWriter.create(outputSchema, outputStream);

        Map<String, String> fieldMapping = createFieldMapping(outputSchema);

        final AvroRecordConverter converter = new AvroRecordConverter(
                inputSchema, outputSchema, fieldMapping);

        for (GenericData.Record record : stream) {
            try {
                GenericData.Record converted = converter.convert(record);
                dataFileWriter.append(converted);
                //written.incrementAndGet();
            } catch (AvroRecordConverter.AvroConversionException e) {
                /*
                failures.add(e);
                getLogger().error(
                        "Error converting data: "
                                + e.getMessage());
                badRecords.add(record);

                 */
            }
            dataFileWriter.flush();
        }

    }

    static Map<String, String> createFieldMapping(Schema outputSchema){
        Map<String, String> fieldMapping = new HashMap<>();
        for (Schema.Field field: outputSchema.getFields()) {
            String flattenSource = field.getProp("flatten_source");
            if (StringUtils.isBlank(flattenSource)) {
                continue;
            }
          fieldMapping.put(flattenSource, field.name());
        }
        return fieldMapping;
    }


}
