package uk.co.threefi.connect.http.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.kafka.test.KafkaBroker;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.ClassRule;
import org.junit.Test;
import uk.co.threefi.connect.http.HttpResponse;

public class KafkaClientTest {

    @ClassRule
    public static final SharedKafkaTestResource kafkaTestHelper = new SharedKafkaTestResource();

    private MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();

    @Test
    public void canPublishToKafka()
          throws ExecutionException, InterruptedException, TimeoutException, IOException, RestClientException {
        final String responseTopic = "response.topic";
        final String key = "123468";
        HttpResponse httpResponse =
              new HttpResponse(201, "http://testURL", "No Content", "Body");

        KafkaClient kafkaClient = getKafkaClient();
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

    @Test(expected = ExecutionException.class)
    public void willThrowExceptionWhenUnableToPublish()
          throws Exception {
        final String responseTopic = "response.topic";
        final String key = "123468";
        HttpResponse httpResponse =
              new HttpResponse(201, "http://testURL", "No Content", "Body");

        KafkaClient kafkaClient = getKafkaClient();
        for (KafkaBroker kafkaBroker : kafkaTestHelper.getKafkaBrokers()) {
            kafkaBroker.stop();
        }
        kafkaClient.publish(key, responseTopic, httpResponse);
    }

    private KafkaClient getKafkaClient() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
              kafkaTestHelper.getKafkaConnectString());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer");
        properties
              .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url", "http://test");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        ProducerConfig producerConfig = new ProducerConfig(properties);

        Map<String, Object> serializerProperties = new HashMap<>();
        serializerProperties.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, true);
        serializerProperties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "nothing");
        KafkaAvroSerializer serializer = new KafkaAvroSerializer(mockSchemaRegistryClient);
        serializer.configure(serializerProperties, false);

        return new KafkaClient(producerConfig, null, serializer);
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