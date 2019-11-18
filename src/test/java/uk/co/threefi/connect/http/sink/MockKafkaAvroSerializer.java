package uk.co.threefi.connect.http.sink;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import uk.co.threefi.connect.http.HttpResponse;

public class MockKafkaAvroSerializer extends KafkaAvroSerializer {

  public MockKafkaAvroSerializer() throws IOException, RestClientException {
    super();
    super.schemaRegistry = obtainMockSchemaRegistryClient();
  }

  private MockSchemaRegistryClient obtainMockSchemaRegistryClient()
      throws IOException, RestClientException {
    final MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
    mockSchemaRegistryClient.register("response.topic-value", HttpResponse.getClassSchema());
    return mockSchemaRegistryClient;
  }
}
