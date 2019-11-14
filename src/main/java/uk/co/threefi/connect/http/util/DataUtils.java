package uk.co.threefi.connect.http.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonElement;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

public class DataUtils {

    public static boolean isBatchResponse(final JsonElement jsonElement) {
        return jsonElement.isJsonArray();
    }

    public static boolean isValidJson(final String jsonInString) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(jsonInString);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static String getKey(final SinkRecord record) {
        return record.key() == null ? StringUtils.EMPTY : StringUtils.trim(record.key().toString());
    }

    public static String buildJsonFromStruct(final Struct struct, final List<String> filter) {
        final JsonNode jsonNode = new SimpleJsonConverter().fromConnectData(struct.schema(), struct);
        stripNulls(jsonNode);
        ((ObjectNode) jsonNode).remove(filter);
        return jsonNode.toString();
    }

    private static void stripNulls(final JsonNode node) {
        final Iterator<JsonNode> it = node.iterator();
        while (it.hasNext()) {
            JsonNode child = it.next();
            if (child.isNull()) {
                it.remove();
            } else {
                stripNulls(child);
            }
        }
    }
}
