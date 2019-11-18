package uk.co.threefi.connect.http.sink.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ResponseKafkaClient extends KafkaClient {

  public ResponseKafkaClient(final ProducerConfig responseProducerConfig) {
    producer = new KafkaProducer<>(responseProducerConfig.originals());
  }

  public void publishResponse(final ProducerRecord<Object, Object> producerRecord)
      throws InterruptedException, ExecutionException, TimeoutException {
    publishRecord(producerRecord);
  }
}
