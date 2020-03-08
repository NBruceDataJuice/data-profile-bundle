package io.datajuice.nifi.processors.descriptor;

import org.apache.avro.reflect.Nullable;

public class Profile {
    @Nullable StringColumn stringColumn;
    @Nullable BooleanColumn booleanColumn;
    @Nullable NumericColumn numericColumn;
    String columnName;

    public Profile(){}

    public Profile(String columnName, BooleanColumn booleanColumn) {
        this.columnName = columnName;
        this.booleanColumn = booleanColumn;
    }

    public Profile(String columnName, NumericColumn numericColumn) {
        this.columnName = columnName;
        this.numericColumn = numericColumn;
    }

    public Profile(String columnName, StringColumn stringColumn) {
        this.columnName = columnName;
        this.stringColumn = stringColumn;
    }
}
