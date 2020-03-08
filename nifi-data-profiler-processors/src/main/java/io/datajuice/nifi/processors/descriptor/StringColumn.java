package io.datajuice.nifi.processors.descriptor;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public class StringColumn {

    int numRows;
    int nullRows;
    BigInteger totalLength;
    int totalWhitespace;
    int totalTokens;
    Map<String, Integer> valueCounter;

    public StringColumn(String columnName, DataFileStream<GenericData.Record> dataFileStream){
        describeString(columnName, dataFileStream);

    }

    // TODO expand on this. Add the option of more in depth stats. Also, add in a property to make this optional
    // TODO need a way to break out of this based on the size of the strings. Should be configurable
    private void describeString(String column, DataFileStream<GenericData.Record> stream){
        numRows = 0;
        nullRows = 0;
        totalLength = BigInteger.ZERO;
        totalWhitespace = 0;
        totalTokens = 0;
        valueCounter = new HashMap<>();

        for (GenericRecord record: stream){

            if( record.get(column) == null){
                nullRows++;
            } else {
                String value = record.get(column).toString();
                valueCounter.putIfAbsent(value, 0);
                valueCounter.put(value, valueCounter.get(value) + 1);

                totalLength.add( new BigInteger(String.valueOf(value.length())) );
                for( int i = 0; i < value.length(); i++ ) {
                    if( Character.isWhitespace(value.charAt(i)) ){
                        totalWhitespace++;
                    }
                }
                // TODO would be great to make this smarter
                totalTokens += value.split(" ").length;
            }
            numRows++;
        }
    }

    public int getNumRows() {
        return numRows;
    }

    public int getNullRows() {
        return nullRows;
    }

    public BigInteger getTotalLength() {
        return totalLength;
    }

    public int getTotalWhitespace() {
        return totalWhitespace;
    }

    public int getTotalTokens() {
        return totalTokens;
    }

    public Map<String, Integer> getValueCounter() {
        return valueCounter;
    }
}
