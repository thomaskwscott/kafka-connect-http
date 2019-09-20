package uk.co.threefi.connect.http.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.kafka.test.KafkaBroker;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.ClassRule;
import org.junit.Test;
import uk.co.threefi.connect.http.HttpResponse;

public class KafkaClientTest {

    @ClassRule
    public static final SharedKafkaTestResource kafkaTestHelper = new SharedKafkaTestResource();

    @Test
    public void canPublishToKafka()
          throws ExecutionException, InterruptedException, TimeoutException {
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

        Record record = getRecord(records);
        assertThat(record.get("statusCode")).isEqualTo(httpResponse.getStatusCode());
        assertThat(record.get("sourceUrl").toString()).isEqualTo(httpResponse.getSourceUrl());
        assertThat(record.get("statusMessage").toString())
              .isEqualTo(httpResponse.getStatusMessage());
        assertThat(record.get("messageBody").toString()).isEqualTo(httpResponse.getMessageBody());
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
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        ProducerConfig producerConfig = new ProducerConfig(properties);

        return new KafkaClient(producerConfig);
    }

    private Record getRecord(List<ConsumerRecord<byte[], byte[]>> records) {
        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
              "http://localhost:8081");
        deserializer.configure(config, false);

        return (Record) deserializer
              .deserialize("", records.get(0).value(), HttpResponse.getClassSchema());
    }
}