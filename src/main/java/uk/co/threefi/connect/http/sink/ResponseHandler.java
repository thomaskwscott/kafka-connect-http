package uk.co.threefi.connect.http.sink;

import static uk.co.threefi.connect.http.util.DataUtils.getKey;
import static uk.co.threefi.connect.http.util.DataUtils.getRetriableRecords;
import static uk.co.threefi.connect.http.util.DataUtils.isBatchResponse;
import static uk.co.threefi.connect.http.util.DataUtils.isValidJson;
import static uk.co.threefi.connect.http.util.HttpUtil.isResponseSuccessful;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.threefi.connect.http.HttpResponse;

public class ResponseHandler {

    private static final Logger logger = LoggerFactory.getLogger(ResponseHandler.class);
    private final HttpSinkConfig httpSinkConfig;

    private final KafkaClient responseKafkaClient;
    private final KafkaClient errorKafkaClient;

    public ResponseHandler(HttpSinkConfig httpSinkConfig,
          ProducerConfig responseProducerConfig,
          ProducerConfig errorProducerConfig,
          Pair<Serializer<Object>, Serializer<Object>> responseSerializers,
          Pair<Serializer<Object>, Serializer<Object>> errorSerializers) {
        this.httpSinkConfig = httpSinkConfig;
        responseKafkaClient = new KafkaClient(responseProducerConfig, responseSerializers);
        errorKafkaClient = new KafkaClient(errorProducerConfig, errorSerializers);
    }

    public ResponseHandler(HttpSinkConfig httpSinkConfig, ProducerConfig responseProducerConfig,
          ProducerConfig errorProducerConfig) {
        this.httpSinkConfig = httpSinkConfig;
        responseKafkaClient = new KafkaClient(responseProducerConfig);
        errorKafkaClient = new KafkaClient(errorProducerConfig);
    }

    public ResponseHandler(HttpSinkConfig httpSinkConfig, KafkaClient responseKafkaClient,
          KafkaClient errorKafkaClient) {
        this.httpSinkConfig = httpSinkConfig;
        this.responseKafkaClient = responseKafkaClient;
        this.errorKafkaClient = errorKafkaClient;
    }


    public Set<RetriableError> processResponse(Response response, String key, String body,
          String formattedUrl) {
        if (!httpSinkConfig.responseTopic.isEmpty()) {
            sendToResponseTopic(response, formattedUrl, key);
        }
        return retrieveErrors(response, key, formattedUrl, body);
    }

    public void handleErrors(Collection<SinkRecord> records, Set<RetriableError> retriableErrors)
          throws ConnectException {
        getRetriableRecords(records, retriableErrors).stream()
              .map(record -> getResponseErrorForRecord(record, retriableErrors))
              .forEach(this::publishError);
    }

    public HttpSinkConfig getHttpSinkConfig() {
        return httpSinkConfig;
    }

    private RetriableError getResponseErrorForRecord(SinkRecord record,
          Set<RetriableError> retriableErrors) {
        String recordKey = getKey(record);
        Optional<String> errorMessage = retriableErrors.stream()
              .filter(responseError -> StringUtils.equals(responseError.getRecordKey(), recordKey))
              .map(RetriableError::getErrorMessage)
              .findAny();
        return new RetriableError(record, errorMessage.orElse(StringUtils.EMPTY));
    }

    private void publishError(RetriableError retriableError) {
        try {
            errorKafkaClient.publishError(httpSinkConfig, retriableError);
        } catch (Exception e) {
            logger.error("Something failed while publishing error");
            throw new RuntimeException(e);
        }
    }

    private void sendToResponseTopic(Response response, String formattedUrl, String key) {
        String statusMessage = response.getStatusMessage() == null
              ? StringUtils.EMPTY
              : response.getStatusMessage();
        String responseBody = response.getBody() == null ? StringUtils.EMPTY : response.getBody();

        HttpResponse httpResponse = new HttpResponse(response.getStatusCode(), formattedUrl,
              statusMessage, responseBody);
        try {
            responseKafkaClient.publish(key, httpSinkConfig.responseTopic, httpResponse);
        } catch (ExecutionException | InterruptedException | TimeoutException exception) {
            logger.error(String.format(
                  "Unable to publish batch with key %s to response topic", key), exception);
        }
    }

    private Set<RetriableError> retrieveErrors(Response response, String key, String formattedUrl,
          String body) {

        Set<RetriableError> retriableErrors = isValidJson(response.getBody())
              ? processJsonResponse(response, key)
              : processUnformattedResponse(response, key);

        if (!retriableErrors.isEmpty()) {
            logger.info("Response error handler found at least one error and will retry");
            logger.debug(String.format(
                  "HTTP Response code: %s %s %s, Submitted payload: %s, url: %s",
                  response.getStatusCode(), response.getStatusMessage(), response.getBody(),
                  body, formattedUrl));
        }
        logger.debug("Response errors have been processed");
        return retriableErrors;
    }

    private Set<RetriableError> processUnformattedResponse(Response response, String key) {
        if (!isResponseSuccessful(response)) {
            logger.debug("Found Unformatted Response");
            String responseBody =
                  response.getBody() == null ? StringUtils.EMPTY : response.getBody();
            return Stream.of(new RetriableError(key, responseBody))
                  .collect(Collectors.toSet());
        }
        return new HashSet<>();
    }

    private Set<RetriableError> processJsonResponse(Response response, String key) {
        logger.debug("Found Json Response : " + response.getBody());

        Set<RetriableError> retriableErrors = new HashSet<>();
        JsonElement jsonTree = new JsonParser().parse(response.getBody());
        JsonElement responseBody = Optional
                .ofNullable(jsonTree.getAsJsonObject().get(httpSinkConfig.responseBody))
                .orElse(jsonTree);

        if (isBatchResponse(responseBody)) {
            JsonArray batchResponse = responseBody.getAsJsonArray();
            batchResponse.forEach(element -> processBatchResponseElement(element, retriableErrors));
        } else {
            JsonObject jsonResponse = responseBody.getAsJsonObject();
            if (!isResponseSuccessful(response)) {
                String bodyString =
                      jsonResponse.isJsonNull() ? StringUtils.EMPTY : jsonResponse.toString();
                retriableErrors.add(new RetriableError(key, bodyString));
            }
        }
        return retriableErrors;
    }

    private void processBatchResponseElement(JsonElement element,
          Set<RetriableError> retriableErrors) {
        JsonObject response = element.getAsJsonObject();

        int statusCode = response.get(httpSinkConfig.errorBatchResponseStatusCode).getAsInt();
        if (!isResponseSuccessful(statusCode)) {
            String batchResponseKey = StringUtils.trim(response
                  .get(httpSinkConfig.errorBatchResponseKey).getAsString());
            String body =
                  response.get(httpSinkConfig.errorBatchResponseBody).isJsonNull()
                        ? StringUtils.EMPTY
                        : response.get(httpSinkConfig.errorBatchResponseBody).toString();
            retriableErrors.add(new RetriableError(batchResponseKey, body));
        }
    }
}
