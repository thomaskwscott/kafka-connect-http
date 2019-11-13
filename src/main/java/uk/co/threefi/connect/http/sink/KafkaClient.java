package uk.co.threefi.connect.http.sink;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaClient {

    private static final Logger logger = LoggerFactory.getLogger(KafkaClient.class);
    private final KafkaProducer<Object, Object> producer;
    private final ProducerConfig producerConfig;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final HttpSinkConfig httpSinkConfig;


    public KafkaClient(ProducerConfig producerConfig, HttpSinkConfig httpSinkConfig,
          Pair<Serializer<Object>, Serializer<Object>> serializers) {
        producer = new KafkaProducer<>(producerConfig.originals(), serializers.getKey(),
              serializers.getValue());
        this.producerConfig = producerConfig;
        this.httpSinkConfig = httpSinkConfig;

        keyConverter = createConverter(httpSinkConfig.getString(HttpSinkConfig.KEY_CONVERTER),
              true);
        valueConverter = createConverter(httpSinkConfig.getString(HttpSinkConfig.VALUE_CONVERTER),
              false);
    }

    public KafkaClient(ProducerConfig producerConfig, HttpSinkConfig httpSinkConfig) {
        producer = new KafkaProducer<>(producerConfig.originals());
        this.producerConfig = producerConfig;
        this.httpSinkConfig = httpSinkConfig;

        keyConverter = createConverter(httpSinkConfig.getString(HttpSinkConfig.KEY_CONVERTER),
              true);
        valueConverter = createConverter(httpSinkConfig.getString(HttpSinkConfig.VALUE_CONVERTER),
              false);
    }

    public void publish(Object sourceKey, String topic, Object value)
          throws ExecutionException, InterruptedException, TimeoutException {
        publishRecord(new ProducerRecord<>(topic, sourceKey, value));
    }

    public void publishError(RetriableError retriableError)
          throws ExecutionException, InterruptedException, TimeoutException {

        ProducerRecord<Object, Object> producerRecord = obtainSerializedProducerRecord(
              retriableError.getSinkRecord(), httpSinkConfig.errorTopic);
        producerRecord.headers().add(new RecordHeader("errorMessage",
              retriableError.getErrorMessage().getBytes()));
        publishRecord(producerRecord);
    }

    private void publishRecord(ProducerRecord<Object, Object> producerRecord)
          throws InterruptedException, ExecutionException, TimeoutException {
        Object key = getMessageItem(producerRecord.key());
        Object value = getMessageItem(producerRecord.value());

        logger.info("Submitting to topic {} with key {} and value {}",
              producerRecord.topic(), key, value);

        Future<RecordMetadata> response = producer.send(producerRecord);
        response.get(20, TimeUnit.SECONDS);

        logger.info("Message successfully sent to topic {} with key {}",
              producerRecord.topic(), key);
    }


    private ProducerRecord<Object, Object> obtainSerializedProducerRecord(SinkRecord sinkRecord, String topic) {
        byte[] convertedKey = keyConverter
              .fromConnectData(topic, sinkRecord.keySchema(), sinkRecord.key());

        byte[] convertedValue = valueConverter
              .fromConnectData(topic, sinkRecord.valueSchema(), sinkRecord.value());

        return new ProducerRecord<>(topic, convertedKey, convertedValue);
    }

    private Object getMessageItem(Object value) {
        return value instanceof byte[] ? "[Byte array]" : value;
    }

    @SuppressWarnings("unchecked")
    private Converter createConverter(String converterName, boolean isKey) {
        try {
            Class<Converter> converter = (Class<Converter>) Class.forName(converterName);
            Converter converterInstance = converter.getConstructor().newInstance();
            converterInstance.configure(producerConfig.originals(), isKey);
            return converterInstance;
        } catch (ClassNotFoundException e) {
            throw new ConnectException(
                  "Failed to find any class that implements Converter and which name matches "
                        + converterName);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e) {
            logger.error("There was an error while initialising the Converter with class: {}",
                  converterName);
            throw new ConnectException(e);
        }
    }
}
