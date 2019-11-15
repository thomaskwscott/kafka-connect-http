package uk.co.threefi.connect.http.sink.client;

import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Before;
import org.junit.ClassRule;

public class KafkaClientTest {

  @ClassRule
  public static final SharedKafkaTestResource kafkaTestHelper = new SharedKafkaTestResource();

  @Before
  public void clearKafkaTopics() throws ExecutionException, InterruptedException {
    Set<String> topics =
        kafkaTestHelper.getKafkaTestUtils().getAdminClient().listTopics().names().get();
    kafkaTestHelper.getKafkaTestUtils().getAdminClient().deleteTopics(topics);
  }

  protected ProducerConfig getProducerConfig(Class<?> keySerializer, Class<?> valueSerializer) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaTestHelper.getKafkaConnectString());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());
    properties.put("schema.registry.url", "http://test");
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
    properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
    return new ProducerConfig(properties);
  }
}
