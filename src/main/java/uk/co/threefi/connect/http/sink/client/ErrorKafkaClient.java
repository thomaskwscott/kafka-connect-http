package uk.co.threefi.connect.http.sink.client;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.threefi.connect.http.sink.config.HttpSinkConfig;
import uk.co.threefi.connect.http.sink.dto.ResponseError;

public class ErrorKafkaClient extends KafkaClient {

  private static final Logger logger = LoggerFactory.getLogger(ErrorKafkaClient.class);
  private final ProducerConfig producerConfig;

  public ErrorKafkaClient(final ProducerConfig errorProducerConfig) {
    this.producerConfig = errorProducerConfig;
    producer = new KafkaProducer<>(errorProducerConfig.originals());
  }

  public void publishError(final HttpSinkConfig httpSinkConfig, final ResponseError responseError)
      throws ExecutionException, InterruptedException, TimeoutException {

    final ProducerRecord<Object, Object> producerRecord =
        obtainSerializedProducerRecord(
            httpSinkConfig, responseError.getSinkRecord(), httpSinkConfig.errorTopic);

    producerRecord
        .headers()
        .add(new RecordHeader("errorMessage", responseError.getErrorMessage().getBytes()));

    publishRecord(producerRecord);
  }

  private ProducerRecord<Object, Object> obtainSerializedProducerRecord(
      final HttpSinkConfig httpSinkConfig, final SinkRecord sinkRecord, final String topic) {

    final Converter keyConverter =
        createConverter(httpSinkConfig, HttpSinkConfig.KEY_CONVERTER, true);
    final byte[] convertedKey =
        obtainConvertedValue(keyConverter, topic, sinkRecord.keySchema(), sinkRecord.key());

    final Converter valueConverter =
        createConverter(httpSinkConfig, HttpSinkConfig.VALUE_CONVERTER, false);
    final byte[] convertedValue =
        obtainConvertedValue(valueConverter, topic, sinkRecord.valueSchema(), sinkRecord.value());

    return new ProducerRecord<>(topic, convertedKey, convertedValue);
  }

  private byte[] obtainConvertedValue(
      final Converter converter, final String topic, final Schema schema, final Object value) {
    return converter.fromConnectData(topic, schema, value);
  }

  private Converter createConverter(
      final HttpSinkConfig httpSinkConfig, final String converterConfig, final boolean isKey) {
    final String converterName = httpSinkConfig.getString(converterConfig);

    try {
      return tryToInstantiateConverter(converterName, isKey);
    } catch (ClassNotFoundException e) {
      throw new ConnectException(
          "Failed to find any class that implements Converter and which name matches "
              + converterName);
    } catch (IllegalAccessException
        | InstantiationException
        | InvocationTargetException
        | NoSuchMethodException e) {
      logger.error(
          "There was an error while initialising the Converter with class: {}", converterName);
      throw new ConnectException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private Converter tryToInstantiateConverter(final String converterName, final boolean isKey)
      throws NoSuchMethodException, ClassNotFoundException, IllegalAccessException,
          InvocationTargetException, InstantiationException {

    final Class<Converter> converter = (Class<Converter>) Class.forName(converterName);
    final Converter converterInstance = converter.getConstructor().newInstance();
    converterInstance.configure(producerConfig.originals(), isKey);
    return converterInstance;
  }
}
