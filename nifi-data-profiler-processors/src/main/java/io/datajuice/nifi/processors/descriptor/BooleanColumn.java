package io.datajuice.nifi.processors.descriptor;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class BooleanColumn {
    int numRows;
    int nullRows;
    int trueRows;
    int falseRows;

    public BooleanColumn(String column, DataFileStream<GenericData.Record> stream){
        describeBoolean(column, stream);

    }

    private void describeBoolean(String column, DataFileStream<GenericData.Record> stream) {
        for (GenericRecord record: stream) {
            if(record.get(column) == null) {
                nullRows++;
            }
            else if ( (Boolean) record.get(column)) {
                trueRows++;
            }
            numRows++;
        }
        falseRows = numRows - nullRows - trueRows;
    }

    public int getNumRows() {
        return numRows;
    }

    public int getNullRows() {
        return nullRows;
    }

    public int getTrueRows() {
        return trueRows;
    }

    public int getFalseRows() {
        return falseRows;
    }

}
