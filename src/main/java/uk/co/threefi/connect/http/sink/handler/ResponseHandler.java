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
import uk.co.threefi.connect.http.sink.dto.RetriableError;

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

  public Set<RetriableError> processResponse(
      final Response response, final String key, String body, final String formattedUrl)
      throws InterruptedException, ExecutionException, TimeoutException {

    if (!httpSinkConfig.responseTopic.isEmpty()) {
      sendToResponseTopic(response, formattedUrl, key);
    }
    return retrieveErrors(response, key, formattedUrl, body);
  }

  public void handleErrors(
      final Collection<SinkRecord> records, final Set<RetriableError> responseErrors) {
    records.stream()
        .filter(
            record ->
                responseErrors.stream()
                    .map(RetriableError::getRecordKey)
                    .anyMatch(key -> StringUtils.equals(getKey(record), key)))
        .map(record -> getResponseErrorForRecord(record, responseErrors))
        .forEach(this::publishError);
  }

  public HttpSinkConfig getHttpSinkConfig() {
    return httpSinkConfig;
  }

  private RetriableError getResponseErrorForRecord(
      final SinkRecord record, final Set<RetriableError> responseErrors) {
    final String recordKey = getKey(record);
    final Optional<String> errorMessage =
        responseErrors.stream()
            .filter(retriableError -> StringUtils.equals(retriableError.getRecordKey(), recordKey))
            .map(RetriableError::getErrorMessage)
            .findAny();
    return new RetriableError(record, errorMessage.orElse(StringUtils.EMPTY));
  }

  private void publishError(final RetriableError retriableError) {
    try {
      errorKafkaClient.publishError(httpSinkConfig, retriableError);
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

  private Set<RetriableError> retrieveErrors(
      final Response response, final String key, final String formattedUrl, final String body) {
    final Set<RetriableError> retriableErrors =
        isValidJson(response.getBody())
            ? processJsonResponse(response, key)
            : processUnformattedResponse(response, key);

    if (!retriableErrors.isEmpty()) {
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
    return retriableErrors;
  }

  private Set<RetriableError> processUnformattedResponse(
      final Response response, final String key) {
    if (!isResponseSuccessful(response)) {
      logger.debug("Found Unformatted Response");
      final String responseBody =
          response.getBody() == null ? StringUtils.EMPTY : response.getBody();
      return Stream.of(new RetriableError(key, responseBody)).collect(Collectors.toSet());
    }
    return new HashSet<>();
  }

  private Set<RetriableError> processJsonResponse(final Response response, final String key) {
    logger.debug("Found Json Response : {}", response.getBody());

    final JsonElement jsonTree = new JsonParser().parse(response.getBody());
    final JsonElement responseBody =
        Optional.ofNullable(jsonTree.getAsJsonObject().get(httpSinkConfig.responseBody))
            .orElse(jsonTree);
    final Set<RetriableError> retriableErrors = new HashSet<>();

    if (isBatchResponse(responseBody)) {
      final JsonArray batchResponse = responseBody.getAsJsonArray();
      batchResponse.forEach(element -> processBatchResponseElement(element, retriableErrors));
    } else {
      final JsonObject jsonResponse = responseBody.getAsJsonObject();
      if (!isResponseSuccessful(response)) {
        final String bodyString =
            jsonResponse.isJsonNull() ? StringUtils.EMPTY : jsonResponse.toString();
        retriableErrors.add(new RetriableError(key, bodyString));
      }
    }
    return retriableErrors;
  }

  private void processBatchResponseElement(
      final JsonElement element, final Set<RetriableError> retriableErrors) {
    final JsonObject response = element.getAsJsonObject();

    final int statusCode = response.get(httpSinkConfig.errorBatchResponseStatusCode).getAsInt();
    if (!isResponseSuccessful(statusCode)) {
      addErrorToResponseErrors(response, retriableErrors);
    }
  }

  private void addErrorToResponseErrors(
      final JsonObject response, final Set<RetriableError> retriableErrors) {
    final String batchResponseKey =
        StringUtils.trim(response.get(httpSinkConfig.errorBatchResponseKey).getAsString());
    final String body =
        response.get(httpSinkConfig.errorBatchResponseBody).isJsonNull()
            ? StringUtils.EMPTY
            : response.get(httpSinkConfig.errorBatchResponseBody).toString();
    retriableErrors.add(new RetriableError(batchResponseKey, body));
  }
}
