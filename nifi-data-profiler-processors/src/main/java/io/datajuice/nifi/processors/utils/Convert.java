package io.datajuice.nifi.processors.utils;

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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.nifi.processors.kite.AvroRecordConverter;

import java.io.*;
import java.util.Map;

import static io.datajuice.nifi.processors.utils.Flatten.createFieldMapping;
import static io.datajuice.nifi.processors.utils.Flatten.flatten;

public class Convert {
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

    public static void convertAvroFile(InputStream inputStream, OutputStream outputStream) throws IOException {
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
}
