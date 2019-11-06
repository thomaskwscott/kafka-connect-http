package uk.co.threefi.connect.http.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class DataUtilsTest {

    private static final Schema TEST_SCHEMA = SchemaBuilder.struct()
            .field("id", SchemaBuilder.string().optional())
            .field("count", SchemaBuilder.int32().optional())
            .field("name", SchemaBuilder.string().optional())
            .field("description", SchemaBuilder.string().optional());

    private static Struct getTestStruct(String id, Integer count, String name, String description) {
        return new Struct(TEST_SCHEMA)
                .put("id", id)
                .put("count", count)
                .put("name", name)
                .put("description", description);
    }

    private static JsonElement getJsonElementFromString(String jsonString) {
        return new JsonParser().parse(jsonString);
    }

    private static SinkRecord getTestSinkRecord(Object key) {
        return new SinkRecord("topic", 0, null, key, null, "value", 0);

    }


    @Test
    public void shouldBeBatchResponse() {
        String inputJson = "[{\"key\":\"value\"}]";
        assertTrue(DataUtils.isBatchResponse(getJsonElementFromString(inputJson)));
    }

    @Test
    public void shouldBeBatchResponseTwoElementList() {
        String inputJson = "[{\"key\":\"value\",\"key2\":\"value2\"}]";
        assertTrue(DataUtils.isBatchResponse(getJsonElementFromString(inputJson)));
    }

    @Test
    public void shouldNotBeBatchResponse() {
        String inputJson = "{\"key\":\"value\"}";
        assertFalse(DataUtils.isBatchResponse(getJsonElementFromString(inputJson)));
    }

    @Test
    public void shouldBeValidJson() {
        String inputJson = "{\"key\":\"value\"}";
        assertTrue(DataUtils.isValidJson(inputJson));
    }

    @Test
    public void shouldNotBeValidJson() {
        String inputJson = "{\"key\",\"key2\"}";
        assertFalse(DataUtils.isValidJson(inputJson));
    }

    @Test
    public void getKeyString() {
        Object inputKey = "key";
        assertEquals(inputKey, DataUtils.getKey(getTestSinkRecord(inputKey)));
    }

    @Test
    public void getKeyNull() {
        Object inputKey = null;
        Object expectedKey = "";
        assertEquals(expectedKey, DataUtils.getKey(getTestSinkRecord(inputKey)));
    }

    @Test
    public void getKeyEmptyString() {
        Object inputKey = "";
        assertEquals(inputKey, DataUtils.getKey(getTestSinkRecord(inputKey)));
    }


    @Test
    public void buildJsonFromStructRemoveMultipleNulls() {
        String expectedOutputJson = "{\"id\":\"myID\",\"count\":1}";
        Struct struct = getTestStruct("myID", 1, null, null);

        assertEquals(
                expectedOutputJson,
                DataUtils.buildJsonFromStruct(struct, Collections.singletonList("")));
    }

    @Test
    public void buildJsonFromStructRemoveNullsNoNulls() {
        String expectedOutputJson = "{\"id\":\"myID\",\"count\":1,\"name\":\"myName\",\"description\":\"myDescription\"}";
        Struct struct = getTestStruct("myID", 1, "myName", "myDescription");

        assertEquals(
                expectedOutputJson,
                DataUtils.buildJsonFromStruct(struct, Collections.singletonList("")));
    }

    @Test
    public void buildJsonFromStructWithFilter() {
        String expectedOutputJson = "{\"count\":1,\"name\":\"myName\",\"description\":\"myDescription\"}";
        Struct struct = getTestStruct("myID", 1, "myName", "myDescription");

        assertEquals(
                expectedOutputJson,
                DataUtils.buildJsonFromStruct(struct, Collections.singletonList("id")));
    }

    @Test
    public void buildJsonFromStructWithFilterThatDoesntExistAndNulls() {
        String expectedOutputJson = "{\"id\":\"myID\",\"count\":1,\"description\":\"myDescription\"}";
        Struct struct = getTestStruct("myID", 1, null, "myDescription");

        assertEquals(
                expectedOutputJson,
                DataUtils.buildJsonFromStruct(struct, Collections.singletonList("not-exists")));
    }

    @Test
    public void buildJsonFromStructWithMultipleFiltersAndNulls() {
        String expectedOutputJson = "{\"id\":\"myID\"}";
        Struct struct = getTestStruct("myID", 1, null, null);

        List<String> filters = new ArrayList<>();
        filters.add("not-exists");
        filters.add("count");

        assertEquals(
                expectedOutputJson,
                DataUtils.buildJsonFromStruct(struct, filters));
    }

}