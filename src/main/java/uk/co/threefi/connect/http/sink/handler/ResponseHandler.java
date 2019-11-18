package uk.co.threefi.connect.http.sink.handler;

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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.threefi.connect.http.HttpResponse;
import uk.co.threefi.connect.http.sink.client.ErrorKafkaClient;
import uk.co.threefi.connect.http.sink.client.ResponseKafkaClient;
import uk.co.threefi.connect.http.sink.config.HttpSinkConfig;
import uk.co.threefi.connect.http.sink.dto.Response;
import uk.co.threefi.connect.http.sink.dto.ResponseError;

public class ResponseHandler {

  private static final Logger logger = LoggerFactory.getLogger(ResponseHandler.class);
  private final HttpSinkConfig httpSinkConfig;

  private final ResponseKafkaClient responseKafkaClient;
  private final ErrorKafkaClient errorKafkaClient;

  public ResponseHandler(
      final HttpSinkConfig httpSinkConfig,
      final ResponseKafkaClient responseKafkaClient,
      final ErrorKafkaClient errorKafkaClient) {
    this.httpSinkConfig = httpSinkConfig;
    this.responseKafkaClient = responseKafkaClient;
    this.errorKafkaClient = errorKafkaClient;
  }

  public Set<ResponseError> processResponse(
      final Response response, final String key, String body, final String formattedUrl)
      throws InterruptedException, ExecutionException, TimeoutException {

    if (!httpSinkConfig.responseTopic.isEmpty()) {
      sendToResponseTopic(response, formattedUrl, key);
    }
    return retrieveErrors(response, key, formattedUrl, body);
  }

  public void handleErrors(
      final Collection<SinkRecord> records, final Set<ResponseError> responseErrors) {
    records.stream()
        .filter(
            record ->
                responseErrors.stream()
                    .map(ResponseError::getRecordKey)
                    .anyMatch(key -> StringUtils.equals(getKey(record), key)))
        .map(record -> getResponseErrorForRecord(record, responseErrors))
        .forEach(this::publishError);
  }

  public HttpSinkConfig getHttpSinkConfig() {
    return httpSinkConfig;
  }

  private ResponseError getResponseErrorForRecord(
      final SinkRecord record, final Set<ResponseError> responseErrors) {
    final String recordKey = getKey(record);
    final Optional<String> errorMessage =
        responseErrors.stream()
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

  private void sendToResponseTopic(
      final Response response, final String formattedUrl, final String key)
      throws InterruptedException, ExecutionException, TimeoutException {

    final String statusMessage =
        response.getStatusMessage() == null ? StringUtils.EMPTY : response.getStatusMessage();
    final String responseBody = response.getBody() == null ? StringUtils.EMPTY : response.getBody();

    final HttpResponse httpResponse =
        new HttpResponse(response.getStatusCode(), formattedUrl, statusMessage, responseBody);

    responseKafkaClient.publishResponse(
        new ProducerRecord<>(httpSinkConfig.responseTopic, key, httpResponse));
  }

  private Set<ResponseError> retrieveErrors(
      final Response response, final String key, final String formattedUrl, final String body) {
    final Set<ResponseError> responseErrors =
        isValidJson(response.getBody())
            ? processJsonResponse(response, key)
            : processUnformattedResponse(response, key);

    if (!responseErrors.isEmpty()) {
      logger.info("Response error handler found at least one error and will retry");
      logger.info(
          String.format(
              "HTTP Response code: %s %s %s, Submitted payload: %s, url: %s",
              response.getStatusCode(),
              response.getStatusMessage(),
              response.getBody(),
              body,
              formattedUrl));
    }
    logger.info("Response errors have been processed");
    return responseErrors;
  }

  private Set<ResponseError> processUnformattedResponse(final Response response, final String key) {
    if (!isResponseSuccessful(response)) {
      logger.debug("Found Unformatted Response");
      final String responseBody =
          response.getBody() == null ? StringUtils.EMPTY : response.getBody();
      return Stream.of(new ResponseError(key, responseBody)).collect(Collectors.toSet());
    }
    return new HashSet<>();
  }

  private Set<ResponseError> processJsonResponse(final Response response, final String key) {
    logger.debug("Found Json Response : {}", response.getBody());

    final JsonElement jsonTree = new JsonParser().parse(response.getBody());
    final JsonElement responseBody =
        Optional.ofNullable(jsonTree.getAsJsonObject().get(httpSinkConfig.responseBody))
            .orElse(jsonTree);
    final Set<ResponseError> responseErrors = new HashSet<>();

    if (isBatchResponse(responseBody)) {
      final JsonArray batchResponse = responseBody.getAsJsonArray();
      batchResponse.forEach(element -> processBatchResponseElement(element, responseErrors));
    } else {
      final JsonObject jsonResponse = responseBody.getAsJsonObject();
      if (!isResponseSuccessful(response)) {
        final String bodyString =
            jsonResponse.isJsonNull() ? StringUtils.EMPTY : jsonResponse.toString();
        responseErrors.add(new ResponseError(key, bodyString));
      }
    }
    return responseErrors;
  }

  private void processBatchResponseElement(
      final JsonElement element, final Set<ResponseError> responseErrors) {
    final JsonObject response = element.getAsJsonObject();

    final int statusCode = response.get(httpSinkConfig.errorBatchResponseStatusCode).getAsInt();
    if (!isResponseSuccessful(statusCode)) {
      addErrorToResponseErrors(response, responseErrors);
    }
  }

  private void addErrorToResponseErrors(
      final JsonObject response, final Set<ResponseError> responseErrors) {
    final String batchResponseKey =
        StringUtils.trim(response.get(httpSinkConfig.errorBatchResponseKey).getAsString());
    final String body =
        response.get(httpSinkConfig.errorBatchResponseBody).isJsonNull()
            ? StringUtils.EMPTY
            : response.get(httpSinkConfig.errorBatchResponseBody).toString();
    responseErrors.add(new ResponseError(batchResponseKey, body));
  }
}
