package io.datajuice.nifi.processors.data.profiler;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestUtil {

    Util util = new Util();

    private static Schema readSchemaFromJsonFile(String filename)
            throws IOException {
        return new Schema.Parser()
                .parse(TestUtil.class.getClassLoader().getResourceAsStream("flattenAvro/" + filename));
    }



    @Test
    public void testCreateFieldMapping() throws IOException{
        Schema originalSchema = readSchemaFromJsonFile("mockAvroSchema.json");
        Schema newSchema = Util.flatten(originalSchema, true);
        for (Schema.Field field: newSchema.getFields()){
            String flattenSource = field.getProp("flatten_source");
            if (StringUtils.isBlank(flattenSource)) {
                continue;
            }
            System.out.println(flattenSource + "  " + field.name());

        }
    }

    /**
     * Test flattening for Record within another Record
     * Record R1 {
     *  fields: { Record R2 }
     * }
     */
    @Test
    public void testRecordWithinRecord() throws IOException {

        Schema originalSchema = readSchemaFromJsonFile("recordWithinRecord_original.json");
        Schema expectedSchema = readSchemaFromJsonFile("recordWithinRecord_flattened.json");

        Assert.assertEquals(new Util().flatten(originalSchema, false), expectedSchema);
    }

    /**
     * Test flattening for Record within Record within another Record
     * Record R1 {
     *  fields:
     *    { Record R2
     *       fields:
     *         {
     *            Record R3
     *         }
     *    }
     * }
     */
    @Test
    public void testRecordWithinRecordWithinRecord() throws IOException {

        Schema originalSchema = readSchemaFromJsonFile("recordWithinRecordWithinRecord_original.json");
        Schema expectedSchema = readSchemaFromJsonFile("recordWithinRecordWithinRecord_flattened.json");

        Assert.assertEquals(new Util().flatten(originalSchema, false), expectedSchema);
    }

    /**
     * Test flattening for Record within an Option within another Record
     * Record R1 {
     *  fields: { Union [ null, Record R2 ] }
     * }
     */
    @Test
    public void testRecordWithinOptionWithinRecord () throws IOException {

        Schema originalSchema = readSchemaFromJsonFile("recordWithinOptionWithinRecord_original.json");
        Schema expectedSchema = readSchemaFromJsonFile("recordWithinOptionWithinRecord_flattened.json");

        Assert.assertEquals(new Util().flatten(originalSchema, false), expectedSchema);
    }

    /**
     * Test flattening for Record within an Union within another Record
     * Record R1 {
     *  fields: { Union [ Record R2, null ] }
     * }
     */
    @Test
    public void testRecordWithinUnionWithinRecord () throws IOException {

        Schema originalSchema = readSchemaFromJsonFile("recordWithinUnionWithinRecord_original.json");
        Schema expectedSchema = readSchemaFromJsonFile("recordWithinUnionWithinRecord_flattened.json");

        Assert.assertEquals(new Util().flatten(originalSchema, false), expectedSchema);
    }

    /**
     * Test flattening for Option within an Option within another Record
     * Record R1 {
     *  fields: {
     *    Union [ null,
     *            Record 2 {
     *              fields: { Union [ null, Record 3] }
     *            }
     *          ]
     *    }
     * }
     */
    @Test
    public void testOptionWithinOptionWithinRecord () throws IOException {

        Schema originalSchema = readSchemaFromJsonFile("optionWithinOptionWithinRecord_original.json");
        Schema expectedSchema = readSchemaFromJsonFile("optionWithinOptionWithinRecord_flattened.json");

        Assert.assertEquals(new Util().flatten(originalSchema, false), expectedSchema);
    }

    /**
     * Test flattening for a Record within Array within Array
     * (no flattening should happen)
     * Array A1 {
     *   [
     *     Array A2 {
     *       [
     *          Record R1
     *       ]
     *     }
     *   ]
     * }
     */
    @Test
    public void testRecordWithinArrayWithinArray () throws IOException {

        Schema originalSchema = readSchemaFromJsonFile("recordWithinArrayWithinArray_original.json");
        Schema expectedSchema = readSchemaFromJsonFile("recordWithinArrayWithinArray_flattened.json");

        Assert.assertEquals(new Util().flatten(originalSchema, false), expectedSchema);
    }

    /**
     * Test flattening for an Array within Record within Array within Record
     * (no flattening should happen)
     * Record R1 {
     *   fields: { [
     *     Array A1 {
     *       [
     *         Record R2 {
     *           fields: { [
     *             Array A2
     *           ] }
     *         }
     *       ]
     *     }
     *   ] }
     * }
     */
    @Test
    public void testArrayWithinRecordWithinArrayWithinRecord () throws IOException {

        Schema originalSchema = readSchemaFromJsonFile("arrayWithinRecordWithinArrayWithinRecord_original.json");
        Schema expectedSchema = readSchemaFromJsonFile("arrayWithinRecordWithinArrayWithinRecord_flattened.json");

        Assert.assertEquals(new Util().flatten(originalSchema, false), expectedSchema);
    }

    /**
     * Test flattening for a Record within Map within Map
     * (no flattening should happen)
     * Map M1 {
     *   values: {
     *     Map M2 {
     *       values: {
     *          Record R1
     *       }
     *     }
     *   }
     * }
     */
    @Test
    public void testRecordWithinMapWithinMap () throws IOException {

        Schema originalSchema = readSchemaFromJsonFile("recordWithinMapWithinMap_original.json");
        Schema expectedSchema = readSchemaFromJsonFile("recordWithinMapWithinMap_flattened.json");

        Assert.assertEquals(Util.flatten(originalSchema, false), expectedSchema);
    }
}
