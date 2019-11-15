package uk.co.threefi.connect.http.sink.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

public class ResponseKafkaClient extends KafkaClient {

  public ResponseKafkaClient(
      final ProducerConfig responseProducerConfig,
      final Pair<Serializer<Object>, Serializer<Object>> serializers) {
    producer =
        new KafkaProducer<>(
            responseProducerConfig.originals(), serializers.getKey(), serializers.getValue());
  }

  public ResponseKafkaClient(final ProducerConfig responseProducerConfig) {
    producer = new KafkaProducer<>(responseProducerConfig.originals());
  }

  public void publishResponse(final ProducerRecord<Object, Object> producerRecord)
      throws InterruptedException, ExecutionException, TimeoutException {
    publishRecord(producerRecord);
  }
}
