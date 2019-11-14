package uk.co.threefi.connect.http.sink.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.salesforce.kafka.test.KafkaBroker;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import uk.co.threefi.connect.http.HttpResponse;
import uk.co.threefi.connect.http.sink.config.HttpSinkConfig;
import uk.co.threefi.connect.http.sink.dto.ResponseError;

public class KafkaClientTest {

    @ClassRule
    public static final SharedKafkaTestResource kafkaTestHelper = new SharedKafkaTestResource();

    private MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();

    @Before
    public void clearKafkaTopics() throws ExecutionException, InterruptedException {
        Set<String> topics = kafkaTestHelper.getKafkaTestUtils()
              .getAdminClient().listTopics().names().get();
        kafkaTestHelper.getKafkaTestUtils().getAdminClient().deleteTopics(topics);
    }

    @Test
    public void canPublishToKafka()
          throws ExecutionException, InterruptedException, TimeoutException, IOException, RestClientException {
        final String responseTopic = "response.topic";
        final String key = "123468";
        HttpResponse httpResponse =
              new HttpResponse(201, "http://testURL", "No Content", "Body");

        ProducerConfig producerConfig = getProducerConfig(StringSerializer.class, KafkaAvroSerializer.class);

        Map<String, Object> serializerProperties = new HashMap<>();
        serializerProperties.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, true);
        serializerProperties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "nothing");
        KafkaAvroSerializer serializer = new KafkaAvroSerializer(mockSchemaRegistryClient);
        serializer.configure(serializerProperties, false);

        KafkaClient kafkaClient = new KafkaClient(producerConfig, Pair.of(null, serializer));
        kafkaClient.publish(key, responseTopic, httpResponse);

        assertThat(kafkaTestHelper.getKafkaTestUtils().getTopics()).hasSize(1);
        assertThat(kafkaTestHelper.getKafkaTestUtils().getTopics().get(0).name())
              .isEqualTo(responseTopic);

        List<ConsumerRecord<byte[], byte[]>> records = kafkaTestHelper
              .getKafkaTestUtils().consumeAllRecordsFromTopic(responseTopic);
        assertThat(records).hasSize(1);

        HttpResponse retrievedHttpResponse = getHttpResponse(records);
        assertThat(retrievedHttpResponse.getStatusCode()).isEqualTo(httpResponse.getStatusCode());
        assertThat(retrievedHttpResponse.getSourceUrl().toString())
              .isEqualTo(httpResponse.getSourceUrl());
        assertThat(retrievedHttpResponse.getStatusMessage().toString())
              .isEqualTo(httpResponse.getStatusMessage());
        assertThat(retrievedHttpResponse.getMessageBody().toString())
              .isEqualTo(httpResponse.getMessageBody());
    }

    @Test
    @SuppressWarnings({"unchecked","rawtypes"})
    public void canPublishErrorToKafka()
          throws InterruptedException, ExecutionException, TimeoutException {
        final String errorTopic = "error.topic";
        ProducerConfig producerConfig = getProducerConfig(ByteArraySerializer.class,
              ByteArraySerializer.class);

        Serializer serializer = new ByteArraySerializer();
        KafkaClient kafkaClient = new KafkaClient(producerConfig, Pair.of(serializer, serializer));

        Map<String, String> properties = new HashMap<>();
        properties.put(HttpSinkConfig.HTTP_API_URL, "stub");
        properties.put(HttpSinkConfig.KEY_CONVERTER, StringConverter.class.getName());
        properties.put(HttpSinkConfig.VALUE_CONVERTER, StringConverter.class.getName());
        properties.put(HttpSinkConfig.VALUE_CONVERTER_SR_URL, "http://localhost:8081");
        properties.put(HttpSinkConfig.ERROR_TOPIC, errorTopic);

        HttpSinkConfig httpSinkConfig = new HttpSinkConfig(properties);
        SinkRecord sinkRecord = new SinkRecord(
              errorTopic, 0, null, "someKey", null
              , "someValue", 0);
        ResponseError responseError = new ResponseError(sinkRecord, "Error occurred");
        kafkaClient.publishError(httpSinkConfig,responseError);

        assertThat(kafkaTestHelper.getKafkaTestUtils().getTopics()).hasSize(1);
        assertThat(kafkaTestHelper.getKafkaTestUtils().getTopics().get(0).name())
              .isEqualTo(errorTopic);

        List<ConsumerRecord<byte[], byte[]>> records = kafkaTestHelper
              .getKafkaTestUtils().consumeAllRecordsFromTopic(errorTopic);
        assertThat(records).hasSize(1);
    }

    @Test(expected = ConnectException.class)
    public void willThrowExceptionWhenConverterNotFound()
          throws InterruptedException, ExecutionException, TimeoutException {

        ProducerConfig producerConfig = getProducerConfig(StringSerializer.class,
              StringSerializer.class);

        KafkaClient kafkaClient = new KafkaClient(producerConfig);

        Map<String, String> properties = new HashMap<>();
        properties.put(HttpSinkConfig.HTTP_API_URL, "stub");
        properties.put(HttpSinkConfig.KEY_CONVERTER, StringConverter.class.getName());
        properties.put(HttpSinkConfig.VALUE_CONVERTER, "unknown.package.converter");
        properties.put(HttpSinkConfig.VALUE_CONVERTER_SR_URL, "http://localhost:8081");

        HttpSinkConfig httpSinkConfig = new HttpSinkConfig(properties);
        SinkRecord sinkRecord = new SinkRecord(
              "topic", 0, null, "someKey", null
              , "someValue", 0);
        ResponseError responseError = new ResponseError(sinkRecord, "Error occurred");
        kafkaClient.publishError(httpSinkConfig,responseError);
    }

    @Test(expected = ExecutionException.class)
    public void willThrowExceptionWhenUnableToPublish()
          throws Exception {
        final String responseTopic = "response.topic";
        final String key = "123468";
        HttpResponse httpResponse =
              new HttpResponse(201, "http://testURL", "No Content", "Body");

        ProducerConfig producerConfig = getProducerConfig(StringSerializer.class, KafkaAvroSerializer.class);

        Map<String, Object> serializerProperties = new HashMap<>();
        serializerProperties.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, false);
        serializerProperties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "nothing");
        KafkaAvroSerializer serializer = new KafkaAvroSerializer(mockSchemaRegistryClient);
        serializer.configure(serializerProperties, false);

        KafkaClient kafkaClient = new KafkaClient(producerConfig, Pair.of(null, serializer));


        for (KafkaBroker kafkaBroker : kafkaTestHelper.getKafkaBrokers()) {
            kafkaBroker.stop();
        }
        kafkaClient.publish(key, responseTopic, httpResponse);
    }

    private ProducerConfig getProducerConfig(Class<?> keySerializer, Class<?> valueSerializer) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
              kafkaTestHelper.getKafkaConnectString());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              keySerializer.getName());
        properties
              .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    valueSerializer.getName());
        properties.put("schema.registry.url", "http://test");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        return new ProducerConfig(properties);
    }

    private HttpResponse getHttpResponse(List<ConsumerRecord<byte[], byte[]>> records)
          throws IOException, RestClientException {
        Map<String, Object> deserializerProperties = new HashMap<>();
        deserializerProperties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        deserializerProperties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
              "http://test");
        mockSchemaRegistryClient.register("response.topic-value", HttpResponse.getClassSchema());
        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(mockSchemaRegistryClient);
        deserializer.configure(deserializerProperties, false);
        return (HttpResponse) deserializer
              .deserialize("", records.get(0).value(), HttpResponse.getClassSchema());
    }
}