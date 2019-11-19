package uk.co.threefi.connect.http.sink.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.Test;
import uk.co.threefi.connect.http.sink.config.HttpSinkConfig;
import uk.co.threefi.connect.http.sink.dto.RetriableError;

public class ErrorKafkaClientTest extends KafkaClientTest {

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void canPublishErrorToKafka()
      throws InterruptedException, ExecutionException, TimeoutException {
    final String errorTopic = "error.topic";
    ProducerConfig producerConfig =
        getProducerConfig(ByteArraySerializer.class, ByteArraySerializer.class);

    ErrorKafkaClient responseKafkaClient = new ErrorKafkaClient(producerConfig);

    Map<String, String> properties = new HashMap<>();
    properties.put(HttpSinkConfig.HTTP_API_URL, "stub");
    properties.put(HttpSinkConfig.KEY_CONVERTER, StringConverter.class.getName());
    properties.put(HttpSinkConfig.VALUE_CONVERTER, StringConverter.class.getName());
    properties.put(HttpSinkConfig.VALUE_CONVERTER_SR_URL, "http://localhost:8081");
    properties.put(HttpSinkConfig.ERROR_TOPIC, errorTopic);

    HttpSinkConfig httpSinkConfig = new HttpSinkConfig(properties);
    SinkRecord sinkRecord = new SinkRecord(errorTopic, 0, null, "someKey", null, "someValue", 0);
    RetriableError retriableError = new RetriableError(sinkRecord, "Error occurred");
    responseKafkaClient.publishError(httpSinkConfig, retriableError);

    assertThat(kafkaTestHelper.getKafkaTestUtils().getTopics()).hasSize(1);
    assertThat(kafkaTestHelper.getKafkaTestUtils().getTopics().get(0).name()).isEqualTo(errorTopic);

    List<ConsumerRecord<byte[], byte[]>> records =
        kafkaTestHelper.getKafkaTestUtils().consumeAllRecordsFromTopic(errorTopic);
    assertThat(records).hasSize(1);
  }

  @Test(expected = ConnectException.class)
  public void willThrowExceptionWhenConverterNotFound()
      throws InterruptedException, ExecutionException, TimeoutException {

    ProducerConfig producerConfig =
        getProducerConfig(StringSerializer.class, StringSerializer.class);

    ErrorKafkaClient errorKafkaClient = new ErrorKafkaClient(producerConfig);

    Map<String, String> properties = new HashMap<>();
    properties.put(HttpSinkConfig.HTTP_API_URL, "stub");
    properties.put(HttpSinkConfig.KEY_CONVERTER, StringConverter.class.getName());
    properties.put(HttpSinkConfig.VALUE_CONVERTER, "unknown.package.converter");
    properties.put(HttpSinkConfig.VALUE_CONVERTER_SR_URL, "http://localhost:8081");

    HttpSinkConfig httpSinkConfig = new HttpSinkConfig(properties);
    SinkRecord sinkRecord = new SinkRecord("topic", 0, null, "someKey", null, "someValue", 0);
    RetriableError retriableError = new RetriableError(sinkRecord, "Error occurred");
    errorKafkaClient.publishError(httpSinkConfig, retriableError);
  }
}
