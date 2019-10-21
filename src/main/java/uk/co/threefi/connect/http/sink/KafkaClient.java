package uk.co.threefi.connect.http.sink;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.threefi.connect.http.HttpResponse;

public class KafkaClient {

    private static final Logger logger = LoggerFactory.getLogger(KafkaClient.class);
    private final KafkaProducer<String, Object> producer;

    public KafkaClient(ProducerConfig producerConfig,
          Serializer<String> keySerializer,
          KafkaAvroSerializer valueSerializer) {
        producer = new KafkaProducer<>(producerConfig.originals(), keySerializer, valueSerializer);
    }

    public void publish(String sourceKey, String responseTopic, HttpResponse httpResponse)
          throws ExecutionException, InterruptedException, TimeoutException {

        logger.debug("Submitting to topic {} with key {} and payload {}",responseTopic, sourceKey, httpResponse);
        Future<RecordMetadata> response = producer
              .send(new ProducerRecord<>(responseTopic, sourceKey, httpResponse));
        response.get(20, TimeUnit.SECONDS);
        logger.info("Message successfully sent to topic {} with key {}", responseTopic, sourceKey);
    }
}
