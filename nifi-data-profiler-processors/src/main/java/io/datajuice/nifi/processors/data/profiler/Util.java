package io.datajuice.nifi.processors.data.profiler;

import com.sun.org.apache.xpath.internal.operations.Bool;
import com.sun.tools.javah.Gen;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import org.apache.commons.math3.stat.descriptive.*;
import org.apache.commons.math3.stat.descriptive.moment.SecondMoment;
import org.apache.gobblin.util.AvroFlattener;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.kite.AvroRecordConverter;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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

        // TODO add some resiliency here
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

    // TODO Optimizations
    // Slower implementation that treats Avro file like a column store. Maybe the next iteration has a conversion step
    // to turn Avro into Parquet or something. The key point here is that we don't want to keep more data in memory
    // than necessary. So, we are willing to sacrifice compute time for a smaller memory footprint. With that being said
    // this footprint could still get rather large. It is all really about the size of the file. It is important to
    // accurately convey this to the users

    // TODO modify this to write as it reads. We don't need a statsMap.

    // TODO create Avro Schema to do the above
    static void iterateDatatypeMap(ProcessSession session, FlowFile flowFile) {
        AtomicReference<Schema> flatAvroSchema = new AtomicReference<>();

        session.read(flowFile, inputStream -> {
            DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>();
            DataFileStream<GenericData.Record> stream = new DataFileStream<>(inputStream, datumReader);
            flatAvroSchema.set(stream.getSchema());
        });

        Map<String, List<String>> datatypeMap = createDatatypeMapping(flatAvroSchema.get());
        // We are going to go through datatypeMap and for each column present, we are going to get the descriptive stats
        // for that column, 1 column at a time, and add it to the statsMap
        for( Map.Entry<String, List<String>> entry : datatypeMap.entrySet() ){
            for( String column : entry.getValue() ){
                session.read(flowFile, inputStream -> {
                    DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>();
                    DataFileStream<GenericData.Record> stream = new DataFileStream<>(inputStream, datumReader);
                    switch (entry.getKey()) {
                        case "number":
                            descriptiveStatisticsNumeric(column, stream);
                            break;

                        case "boolean":
                            descriptiveStatisticsBoolean(column, stream);
                            break;

                        case "string":
                            descriptiveStatisticsString(column, stream);
                            break;

                        default:
                            break;
                    }

                });
            }
        }
    }

    private static void descriptiveStatisticsNumeric(String column, DataFileStream < GenericData.Record > stream) {
        DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
        int numRows = 0;
        for (GenericRecord record: stream){

            // TODO added to complete tests. 100% sure this needs to be cleaned up
            if ( record.getSchema().getField(column).schema().getType().toString().toLowerCase().equals("long") ){
                descriptiveStatistics.addValue(((Long) record.get(column)).doubleValue());
            } else if ( record.getSchema().getField(column).schema().getType().toString().toLowerCase().equals("int") ){
                descriptiveStatistics.addValue((Integer) record.get(column));
            }
            else {
                descriptiveStatistics.addValue((Double) record.get(column));
            }
            numRows++;
        }

        final long numValues = descriptiveStatistics.getN();
        final double max = descriptiveStatistics.getMax();
        final double min = descriptiveStatistics.getMin();
        final double sum = descriptiveStatistics.getSum();
        final double mean = descriptiveStatistics.getMean();
        final double standardDeviation = descriptiveStatistics.getStandardDeviation();
        final double variance = descriptiveStatistics.getVariance();
        final double geometricMean = descriptiveStatistics.getGeometricMean();
        final double sumOfSquares = descriptiveStatistics.getSumsq();
        final double secondMoment = new SecondMoment().evaluate(descriptiveStatistics.getValues());
        final double kurtosis = descriptiveStatistics.getKurtosis();
        final double skewness = descriptiveStatistics.getSkewness();
        final double median = descriptiveStatistics.getPercentile(50.0);
        final double percentile25 = descriptiveStatistics.getPercentile(25.0);
        final double percentile75 = descriptiveStatistics.getPercentile(75.0);
        final double nullPct;
        if (numRows > 0){
            nullPct = 1 - numValues / numRows;
        } else{
            nullPct = -1;
        }

        System.out.println(nullPct);
    }

    private static void descriptiveStatisticsBoolean(String column, DataFileStream < GenericData.Record > stream) {
        int numRows = 0;
        int trueRows = 0;
        int falseRows = 0;
        int nullRows = 0;
        for (GenericRecord record: stream){

            if( record.get(column) == null){
                nullRows++;
            } else if ( (Boolean) record.get(column)){
                trueRows++;
            }else {
                falseRows++;
            }
            numRows++;
        }

        final double nullPct;
        if (numRows > 0){
            nullPct = 1 - (trueRows + falseRows) / numRows;
        } else{
            nullPct = -1;
        }

        System.out.println(nullPct);
    }

    // TODO expand on this. Add the option of more in depth stats. Also, add in a property to make this optional
    // TODO need a way to break out of this based on the size of the strings. Should be configurable
    private static void descriptiveStatisticsString(String column, DataFileStream < GenericData.Record > stream){
        int numRows = 0;
        int nullRows = 0;
        BigInteger totalLength = BigInteger.ZERO;
        int totalWhitespace = 0;
        int totalTokens = 0;

        Map<String, Integer> valueCounter = new HashMap<>();
        for (GenericRecord record: stream){

            if( record.get(column) == null){
                nullRows++;
            } else {
                String value = record.get(column).toString();
                if ( valueCounter.get(value) == null ){
                    valueCounter.put(value, 0);
                }
                valueCounter.put(value, valueCounter.get(value) + 1);

                totalLength.add( new BigInteger(String.valueOf(value.length())) );
                for( int i = 0; i < value.length(); i++ ) {
                    if( Character.isWhitespace(value.charAt(i)) ){
                        totalWhitespace++;
                    }
                }

                // TODO would be great to make this more
                totalTokens += value.split(" ").length;
            }

            numRows++;
        }

        final double nullPct;
        if ( numRows > 0 ){
            nullPct = 1 - nullRows / numRows;
        } else{
            nullPct = -1;
        }

        System.out.println(nullPct);
        for(Map.Entry<String, Integer> entry: valueCounter.entrySet()){
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }

    }

    static void csvToAvro(Schema schema, File file) throws IOException {
        CSVParser csvParser = CSVFormat.DEFAULT.parse(new InputStreamReader(new FileInputStream(file)));
        boolean first = true;

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        //dataFileWriter.setCodec(CodecFactory.snappyCodec());
        dataFileWriter.create(schema, new FileOutputStream(new File("src/test/resources/kaggle/investments_VC_uncompressed.avro")));

        for(CSVRecord record: csvParser.getRecords()){
            if ( first ){
                first = false;
                continue;
            }

            GenericRecord avroRecord = new GenericData.Record(schema);
            int i = 0;
            for(Schema.Field field: schema.getFields()){
                switch (field.schema().getType()){
                    case RECORD:
                        break;
                    case ENUM:
                        break;
                    case ARRAY:
                        break;
                    case MAP:
                        break;
                    case UNION:
                        break;
                    case FIXED:
                        break;
                    case STRING:
                        avroRecord.put(field.name(),record.get(i));
                        break;
                    case BYTES:
                        avroRecord.put(field.name(), record.get(i).getBytes());
                        break;
                    case INT:
                        avroRecord.put(field.name(), Integer.parseInt(record.get(i)));
                        break;
                    case LONG:
                        avroRecord.put(field.name(), Long.parseLong(record.get(i)));
                        break;
                    case FLOAT:
                        avroRecord.put(field.name(), Float.parseFloat(record.get(i)));
                        break;
                    case DOUBLE:
                        avroRecord.put(field.name(), Double.parseDouble(record.get(i)));
                        break;
                    case BOOLEAN:
                        avroRecord.put(field.name(), Boolean.parseBoolean(record.get(i)));
                        break;
                    case NULL:
                        break;
                }

                i++;
            }
            dataFileWriter.append(avroRecord);
        }

        dataFileWriter.close();
    }
}

