package uk.co.threefi.connect.http.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.co.threefi.connect.http.sink.HttpSinkConfig.ERROR_BATCH_RESPONSE_BODY;
import static uk.co.threefi.connect.http.sink.HttpSinkConfig.ERROR_BATCH_RESPONSE_KEY;
import static uk.co.threefi.connect.http.sink.HttpSinkConfig.ERROR_BATCH_RESPONSE_STATUS_CODE;
import static uk.co.threefi.connect.http.sink.HttpSinkConfig.RESPONSE_BODY;
import static uk.co.threefi.connect.http.sink.HttpSinkConfig.RESPONSE_PRODUCER;

import com.google.gson.JsonParser;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import uk.co.threefi.connect.http.HttpResponse;
import uk.co.threefi.connect.http.sink.HttpSinkConfig.RequestMethod;

public class ResponseHandlerTest {

    private ResponseHandler responseHandler;

    private final String inputKey = "someKey";
    private final String sourceUrl = "http://testurl";

    @Before
    public void init() {
        HttpSinkConfig httpSinkConfig = new HttpSinkConfig(getProperties());
        ProducerConfig responseProducerConfig = new ProducerConfig(
              Collections.unmodifiableMap(getResponseProducerProperties()));
        ProducerConfig errorProducerConfig = new ProducerConfig(
              Collections.unmodifiableMap(getErrorProducerProperties()));
        responseHandler = new ResponseHandler(httpSinkConfig, responseProducerConfig,
              errorProducerConfig);
    }

    @Test
    public void canRetrieveErrorsForUnformattedResponse()
          throws InterruptedException, ExecutionException, TimeoutException {
        Response response = new Response(401, "fail", "ResponseMessage");
        Set<RetriableError> retriableErrors = responseHandler
              .processResponse(response, inputKey, "someBody", sourceUrl);
        assertEquals(1, retriableErrors.size());
        RetriableError retriableError = (RetriableError) retriableErrors.toArray()[0];
        assertEquals(response.getBody(), retriableError.getErrorMessage());
        assertEquals(inputKey, retriableError.getRecordKey());
    }

    @Test
    public void canReturnNoErrorsForUnformattedResponse()
          throws InterruptedException, ExecutionException, TimeoutException {
        Response response = new Response(200, "pass", "ResponseMessage");
        Set<RetriableError> retriableErrors = responseHandler
              .processResponse(response, inputKey, "someBody", sourceUrl);
        assertTrue(retriableErrors.isEmpty());
    }

    @Test
    public void canRetrieveErrorsForJsonSingleResponse()
          throws InterruptedException, ExecutionException, TimeoutException {
        String body = "{\"statusCode\":200,"
              + "\"sourceUrl\":\"sourceUrl\","
              + "\"statusMessage\":\"OK\"}";
        Response response = new Response(401, "fail", body);
        Set<RetriableError> retriableErrors = responseHandler
              .processResponse(response, inputKey, "someBody", sourceUrl);
        assertEquals(1, retriableErrors.size());
        RetriableError retriableError = (RetriableError) retriableErrors.toArray()[0];
        assertEquals(response.getBody(), retriableError.getErrorMessage());
        assertEquals(inputKey, retriableError.getRecordKey());
    }

    @Test
    public void canReturnNoErrorsForJsonSingleResponse()
          throws InterruptedException, ExecutionException, TimeoutException {
        String body = "{\"statusCode\":200,"
              + "\"sourceUrl\":\"sourceUrl\","
              + "\"statusMessage\":\"OK\"}";
        Response response = new Response(200, "pass", body);
        Set<RetriableError> retriableErrors = responseHandler
              .processResponse(response, inputKey, "someBody", sourceUrl);
        assertTrue(retriableErrors.isEmpty());
    }

    @Test
    public void canRetrieveErrorsForJsonBatchResponse()
          throws InterruptedException, ExecutionException, TimeoutException {
        String key = "121126";
        String batchBody = "{\"body\":[{\"message\":\"Error while updating\","
              + "\"errorCode\":\"INVALID_FIELD\"}],\"httpHeaders\":{},\"httpStatusCode\":400,"
              + "\"referenceId\":\"" + key + "\"}";
        String batchBody2 = "{\"body\":[{\"message\":\"Updated Succesfully\","
              + "\"errorCode\":\"[]]\"}],\"httpHeaders\":{},\"httpStatusCode\":200,"
              + "\"referenceId\":\"121127\"}";
        String body = "{\"compositeResponse\":[" + batchBody + " , " + batchBody2 + "] }";

        Response response = new Response(200, "fail", body);
        Set<RetriableError> retriableErrors = responseHandler
              .processResponse(response, inputKey, "someBody", sourceUrl);
        assertEquals(1, retriableErrors.size());

        RetriableError retriableError = (RetriableError) retriableErrors.toArray()[0];
        String batchErrorMessage = new JsonParser().parse(batchBody)
              .getAsJsonObject().get(getProperties().get(ERROR_BATCH_RESPONSE_BODY))
              .getAsJsonArray().toString();
        assertEquals(batchErrorMessage, retriableError.getErrorMessage());
        assertEquals(key, retriableError.getRecordKey());
    }

    @Test
    public void canReturnNoErrorsForJsonBatchResponse()
          throws InterruptedException, ExecutionException, TimeoutException {
        String batchBody = "{\"body\":[{\"message\":\"Updated Succesfully\","
              + "\"errorCode\":\"INVALID_FIELD\"}],\"httpHeaders\":{},\"httpStatusCode\":200,"
              + "\"referenceId\":\"12116\"}";
        String batchBody2 = "{\"body\":[{\"message\":\"Updated Succesfully\","
              + "\"errorCode\":\"[]]\"}],\"httpHeaders\":{},\"httpStatusCode\":200,"
              + "\"referenceId\":\"121127\"}";
        String body = "{\"compositeResponse\":[" + batchBody + " , " + batchBody2 + "] }";

        Response response = new Response(200, "pass", body);
        Set<RetriableError> retriableErrors = responseHandler
              .processResponse(response, inputKey, "someBody", sourceUrl);
        assertTrue(retriableErrors.isEmpty());
    }

    @Test
    public void canHandleFoundErrors()
          throws InterruptedException, ExecutionException, TimeoutException {
        List<SinkRecord> sinkRecords = new ArrayList<>();
        String payload1 = "someValue";
        sinkRecords.add(new SinkRecord("someTopic1", 0, null, inputKey, null, payload1, 0));

        Set<RetriableError> retriableErrors = Stream
              .of(new RetriableError(inputKey, "There was an error"))
              .collect(Collectors.toSet());

        KafkaClient errorKafkaClient = mock(KafkaClient.class);
        HttpSinkConfig httpSinkConfig = responseHandler.getHttpSinkConfig();
        ResponseHandler responseHandler = new ResponseHandler(httpSinkConfig, null,
              errorKafkaClient);
        responseHandler.handleErrors(sinkRecords, retriableErrors);

        verify(errorKafkaClient).publishError(any());
    }

    @Test
    public void canSendToResponseTopic()
          throws InterruptedException, ExecutionException, TimeoutException {
        Response response = new Response(200, "ok", "ResponseMessage");

        Map<String, String> properties = getProperties();
        properties.put(HttpSinkConfig.RESPONSE_TOPIC, "response.topic");
        HttpSinkConfig httpSinkConfig = new HttpSinkConfig(properties);

        KafkaClient responseKafkaClient = mock(KafkaClient.class);
        ResponseHandler responseHandler = new ResponseHandler(httpSinkConfig, responseKafkaClient,
              null);
        responseHandler.processResponse(response, inputKey, "body", sourceUrl);

        HttpResponse httpResponse = new HttpResponse(response.getStatusCode(), sourceUrl,
              response.getStatusMessage(), response.getBody());
        verify(responseKafkaClient).publish(inputKey, httpSinkConfig.responseTopic, httpResponse);
    }


    @Test(expected = RuntimeException.class)
    public void canThrowRuntimeExceptionWhenErrorPublishing(){
        List<SinkRecord> sinkRecords = new ArrayList<>();
        String payload1 = "someValue";
        sinkRecords.add(new SinkRecord("someTopic1", 0, null, inputKey, null, payload1, 0));
        Set<RetriableError> retriableErrors = Stream
              .of(new RetriableError(inputKey, "There was an error"))
              .collect(Collectors.toSet());
        responseHandler.handleErrors(sinkRecords, retriableErrors);
    }

    private Map<String, String> getResponseProducerProperties() {
        Map<String, String> responseProducerProperties = getCommonProducerProperties();
        responseProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              StringSerializer.class.getName());
        responseProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              KafkaAvroSerializer.class.getName());
        return responseProducerProperties;
    }

    private Map<String, String> getErrorProducerProperties() {
        Map<String, String> errorProducerProperties = getCommonProducerProperties();
        errorProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              ByteArraySerializer.class.getName());
        errorProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              ByteArraySerializer.class.getName());
        return errorProducerProperties;
    }

    private Map<String, String> getCommonProducerProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(HttpSinkConfig.REQUEST_METHOD, RequestMethod.POST.toString());
        properties.put(HttpSinkConfig.HEADERS, "Content-Type:application/json");
        properties.put(ProducerConfig.RETRIES_CONFIG, "1");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        properties.put("schema.registry.url", "http://localhost:8081");
        return properties;
    }

    private Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(HttpSinkConfig.HTTP_API_URL, "stub");
        properties.put(HttpSinkConfig.RETRY_BACKOFF_MS, String.valueOf(1000));

        properties.put(RESPONSE_PRODUCER + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
              "http://localhost:9092");
        properties.put(RESPONSE_PRODUCER + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer");
        properties
              .put(RESPONSE_PRODUCER + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    KafkaAvroSerializer.class.getName());
        properties.put(RESPONSE_PRODUCER + "schema.registry.url", "http://localhost:8081");
        properties.put(RESPONSE_BODY, "compositeResponse");
        properties.put(ERROR_BATCH_RESPONSE_KEY, "referenceId");
        properties.put(ERROR_BATCH_RESPONSE_BODY, "body");
        properties.put(ERROR_BATCH_RESPONSE_STATUS_CODE, "httpStatusCode");

        return properties;
    }
}
