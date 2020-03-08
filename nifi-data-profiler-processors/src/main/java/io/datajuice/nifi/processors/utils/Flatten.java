package io.datajuice.nifi.processors.utils;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.util.AvroFlattener;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class Flatten {
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
