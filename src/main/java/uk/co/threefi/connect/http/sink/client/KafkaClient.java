package uk.co.threefi.connect.http.sink.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KafkaClient {

  private static final Logger logger = LoggerFactory.getLogger(KafkaClient.class);

  protected KafkaProducer<Object, Object> producer;

  protected void publishRecord(final ProducerRecord<Object, Object> producerRecord)
      throws InterruptedException, ExecutionException, TimeoutException {

    final Object key = getMessageItem(producerRecord.key());
    final Object value = getMessageItem(producerRecord.value());

    logger.info(
        "Submitting to topic {} with key {} and value {}", producerRecord.topic(), key, value);

    final Future<RecordMetadata> response = producer.send(producerRecord);
    response.get(20, TimeUnit.SECONDS);

    logger.info(
        "Message successfully sent to topic {} with key {}",
        producerRecord.topic(),
        producerRecord.key());
  }

  private Object getMessageItem(final Object value) {
    return value instanceof byte[] ? "Byte array" : value;
  }
}
