package uk.co.threefi.connect.http.sink;

import static uk.co.threefi.connect.http.util.DataUtils.getKey;
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


    public Set<ResponseError> processResponse(Response response, String key, String body,
          String formattedUrl) throws InterruptedException, ExecutionException, TimeoutException {
        if (!httpSinkConfig.responseTopic.isEmpty()) {
            sendToResponseTopic(response, formattedUrl, key);
        }
        return retrieveErrors(response, key, formattedUrl, body);
    }

    public void handleErrors(Collection<SinkRecord> records, Set<ResponseError> responseErrors)
          throws ConnectException {
        records.stream()
              .filter(record ->
                    responseErrors.stream()
                          .map(ResponseError::getRecordKey)
                          .anyMatch(key -> StringUtils.equals(getKey(record), key)))
              .map(record -> getResponseErrorForRecord(record, responseErrors))
              .forEach(this::publishError);
    }

    public HttpSinkConfig getHttpSinkConfig() {
        return httpSinkConfig;
    }

    private ResponseError getResponseErrorForRecord(SinkRecord record,
          Set<ResponseError> responseErrors) {
        String recordKey = getKey(record);
        Optional<String> errorMessage = responseErrors.stream()
              .filter(responseError -> StringUtils.equals(responseError.getRecordKey(), recordKey))
              .map(ResponseError::getErrorMessage)
              .findAny();
        return new ResponseError(record, errorMessage.orElse(StringUtils.EMPTY));
    }

    private void publishError(ResponseError responseError) {
        try {
            errorKafkaClient.publishError(httpSinkConfig, responseError);
        } catch (Exception e) {
            logger.error("Something failed while publishing error");
            throw new RuntimeException(e);
        }
    }

    private void sendToResponseTopic(Response response, String formattedUrl, String key)
          throws InterruptedException, ExecutionException, TimeoutException {
        String statusMessage = response.getStatusMessage() == null
              ? StringUtils.EMPTY
              : response.getStatusMessage();
        String responseBody = response.getBody() == null ? StringUtils.EMPTY : response.getBody();

        HttpResponse httpResponse = new HttpResponse(response.getStatusCode(), formattedUrl,
              statusMessage, responseBody);
        responseKafkaClient.publish(key, httpSinkConfig.responseTopic, httpResponse);
    }

    private Set<ResponseError> retrieveErrors(Response response, String key, String formattedUrl,
          String body) {

        Set<ResponseError> responseErrors = isValidJson(response.getBody())
              ? processJsonResponse(response, key)
              : processUnformattedResponse(response, key);

        if (!responseErrors.isEmpty()) {
            logger.info("Response error handler found at least one error and will retry");
            logger.info(String.format(
                  "HTTP Response code: %s %s %s, Submitted payload: %s, url: %s",
                  response.getStatusCode(), response.getStatusMessage(), response.getBody(),
                  body, formattedUrl));
        }
        logger.info("Response errors have been processed");
        return responseErrors;
    }

    private Set<ResponseError> processUnformattedResponse(Response response, String key) {
        if (!isResponseSuccessful(response)) {
            logger.debug("Found Unformatted Response");
            String responseBody =
                  response.getBody() == null ? StringUtils.EMPTY : response.getBody();
            return Stream.of(new ResponseError(key, responseBody))
                  .collect(Collectors.toSet());
        }
        return new HashSet<>();
    }

    private Set<ResponseError> processJsonResponse(Response response, String key) {
        logger.debug("Found Json Response : " + response.getBody());

        Set<ResponseError> responseErrors = new HashSet<>();
        JsonElement jsonTree = new JsonParser().parse(response.getBody());
        JsonElement responseBody = Optional
                .ofNullable(jsonTree.getAsJsonObject().get(httpSinkConfig.responseBody))
                .orElse(jsonTree);

        if (isBatchResponse(responseBody)) {
            JsonArray batchResponse = responseBody.getAsJsonArray();
            batchResponse.forEach(element -> processBatchResponseElement(element, responseErrors));
        } else {
            JsonObject jsonResponse = responseBody.getAsJsonObject();
            if (!isResponseSuccessful(response)) {
                String bodyString =
                      jsonResponse.isJsonNull() ? StringUtils.EMPTY : jsonResponse.toString();
                responseErrors.add(new ResponseError(key, bodyString));
            }
        }
        return responseErrors;
    }

    private void processBatchResponseElement(JsonElement element,
          Set<ResponseError> responseErrors) {
        JsonObject response = element.getAsJsonObject();

        int statusCode = response.get(httpSinkConfig.errorBatchResponseStatusCode).getAsInt();
        if (!isResponseSuccessful(statusCode)) {
            String batchResponseKey = StringUtils.trim(response
                  .get(httpSinkConfig.errorBatchResponseKey).getAsString());
            String body =
                  response.get(httpSinkConfig.errorBatchResponseBody).isJsonNull()
                        ? StringUtils.EMPTY
                        : response.get(httpSinkConfig.errorBatchResponseBody).toString();
            responseErrors.add(new ResponseError(batchResponseKey, body));
        }
    }
}
