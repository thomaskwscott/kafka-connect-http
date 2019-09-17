package uk.co.threefi.connect.http.sink;

import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.threefi.connect.http.HttpResponse;

public class KafkaClient {

    private static final Logger logger = LoggerFactory.getLogger(KafkaClient.class);
    private final KafkaProducer<String, HttpResponse> producer;

    public KafkaClient(ProducerConfig producerConfig) {
        producer = new KafkaProducer<>(producerConfig.originals());
    }

    public void publish(String sourceKey, String responseTopic, HttpResponse httpResponse)
          throws ExecutionException, InterruptedException {
        logger.debug("Submitting to topic {} with payload {}", responseTopic, httpResponse);
        producer.send(new ProducerRecord<>(responseTopic, sourceKey, httpResponse)).get();
    }
}
