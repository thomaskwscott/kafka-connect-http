/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.threefi.connect.http.sink;

import static uk.co.threefi.connect.http.sink.HttpSinkConfig.ERROR_PRODUCER;
import static uk.co.threefi.connect.http.sink.HttpSinkConfig.RESPONSE_PRODUCER;
import static uk.co.threefi.connect.http.util.DataUtils.getRetriableRecords;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);

  private HttpSinkConfig httpSinkConfig;
  private ProducerConfig responseProducerConfig;
  private ProducerConfig errorProducerConfig;
  protected HttpApiWriter writer;
  private int remainingRetries;
  protected ResponseHandler responseHandler;

  @Override
  public void start(final Map<String, String> props) {
    log.info("Starting task");
    httpSinkConfig = new HttpSinkConfig(props);
    remainingRetries = httpSinkConfig.maxRetries;

    responseProducerConfig = obtainProducerConfig(props,RESPONSE_PRODUCER, new HashMap<>());
    HashMap<String, String> customProducerProperties = getCustomErrorProducerProperties();
    errorProducerConfig = obtainProducerConfig(props, ERROR_PRODUCER, customProducerProperties);

    try {
      init();
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    final SinkRecord first = records.iterator().next();
    log.trace(
        "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
        + "API...",
          records.size(), first.topic(), first.kafkaPartition(), first.kafkaOffset()
    );
    Set<RetriableError> retriableErrors = writeRecords(records);
    responseHandler.handleErrors(records, retriableErrors);
    remainingRetries = httpSinkConfig.maxRetries;
  }

  private Set<RetriableError> writeRecords(Collection<SinkRecord> records) {
    Set<RetriableError> retriableErrors = writer.write(records);
    if (!retriableErrors.isEmpty() && remainingRetries > 0) {
      log.warn("Write of {} records failed, remainingRetries={}", records.size(), remainingRetries);
      remainingRetries--;

      retriableErrors = writeRecords(getRetriableRecords(records,retriableErrors));
      context.timeout(httpSinkConfig.retryBackoffMs);
    }
    return retriableErrors;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    //ignored
  }

  public void stop() {
    log.info("Stopping task");
  }

  @Override
  public String version() {
    return getClass().getPackage().getImplementationVersion();
  }

  protected void init() throws Exception {
    responseHandler = new ResponseHandler(httpSinkConfig, responseProducerConfig,
          errorProducerConfig);
    writer = new HttpApiWriter(responseHandler);
  }

  private HashMap<String, String> getCustomErrorProducerProperties() {
    HashMap<String,String> customProducerProperties = new HashMap<>();
    customProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          ByteArraySerializer.class.getName());
    customProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          ByteArraySerializer.class.getName());
    return customProducerProperties;
  }

  private Map<String, String> obtainProducerConfigProps(Map<String, String> props,
        String producerType) {
    return props.entrySet().stream()
          .filter(entry -> entry.getKey().startsWith(producerType))
          .collect(Collectors.toMap(
                entry -> entry.getKey().replaceFirst(producerType, StringUtils.EMPTY),
                Entry::getValue));
  }

  private ProducerConfig obtainProducerConfig(Map<String, String> props, String producerType,
        Map<String,String> customProperties) {
    Map<String, String> producerConfigProperties = obtainProducerConfigProps(props, producerType);
    producerConfigProperties.putAll(customProperties);
    return new ProducerConfig(Collections.unmodifiableMap(producerConfigProperties));
  }
}
