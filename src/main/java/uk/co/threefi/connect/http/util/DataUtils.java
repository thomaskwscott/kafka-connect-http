package uk.co.threefi.connect.http.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonElement;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import uk.co.threefi.connect.http.sink.RetriableError;

public class DataUtils {

    public static boolean isBatchResponse(JsonElement jsonElement) {
        return jsonElement.isJsonArray();
    }

    public static boolean isValidJson(String jsonInString) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(jsonInString);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static String getKey(SinkRecord record) {
        return record.key() == null ? StringUtils.EMPTY : StringUtils.trim(record.key().toString());
    }

    public static String buildJsonFromStruct(Struct struct, List<String> filter) {
            JsonNode jsonNode = new SimpleJsonConverter().fromConnectData(struct.schema(), struct);
        stripNulls(jsonNode);
        ((ObjectNode) jsonNode).remove(filter);
        return jsonNode.toString();
    }

    private static void stripNulls(JsonNode node) {
        Iterator<JsonNode> it = node.iterator();
        while (it.hasNext()) {
            JsonNode child = it.next();
            if (child.isNull()) {
                it.remove();
            } else {
                stripNulls(child);
            }
        }
    }

    public static Set<SinkRecord> getRetriableRecords(Collection<SinkRecord> records,
          Set<RetriableError> retriableErrors) {
        return records.stream()
              .filter(record ->
                    retriableErrors.stream()
                          .map(RetriableError::getRecordKey)
                          .anyMatch(key -> StringUtils.equals(getKey(record), key))).collect(
                    Collectors.toSet());
    }
}
