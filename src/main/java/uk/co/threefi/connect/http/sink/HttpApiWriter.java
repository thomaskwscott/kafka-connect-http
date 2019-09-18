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

import java.io.IOException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.threefi.connect.http.HttpResponse;
import uk.co.threefi.connect.http.util.HttpUtil;


public class HttpApiWriter {

    private final JavaNetHttpClient httpClient;
    private final KafkaClient kafkaClient;
    private final HttpSinkConfig httpSinkConfig;
    private static final Logger log = LoggerFactory.getLogger(HttpApiWriter.class);
    private Map<String, List<SinkRecord>> batches = new HashMap<>();


    HttpApiWriter(final HttpSinkConfig httpSinkConfig, ProducerConfig producerConfig)
          throws Exception {
        this.httpSinkConfig = httpSinkConfig;
        PayloadGenerator payloadGenerator = new PayloadGenerator(
              extractPrivateKeyFromConfig(httpSinkConfig),
              httpSinkConfig.salesforceAuthenticationClientId,
              httpSinkConfig.salesforceAuthenticationUsername,
              httpSinkConfig.salesforceAuthenticationRoot);

        SalesforceAuthenticationProvider authenticationProvider =
              new SalesforceAuthenticationProvider(
                    httpSinkConfig.salesforceAuthenticationRoot,
                    new JavaNetHttpClient(),
                    Clock.systemDefaultZone(),
                    payloadGenerator);

        httpClient = new AuthenticatedJavaNetHttpClient(authenticationProvider);
        kafkaClient = new KafkaClient(producerConfig);
    }

    private PrivateKey extractPrivateKeyFromConfig(HttpSinkConfig config)
          throws InvalidKeySpecException, NoSuchAlgorithmException {
        byte[] keyBytes = Base64.getDecoder()
              .decode(config.salesforceAuthenticationPrivateKey
                    .replaceAll("-----BEGIN PRIVATE KEY-----", "")
                    .replaceAll("-----END PRIVATE KEY-----", "")
                    .replaceAll("\\s+", "")
                    .getBytes());
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
        return KeyFactory.getInstance("RSA").generatePrivate(keySpec);
    }

    public void write(final Collection<SinkRecord> records)
          throws IOException, ExecutionException, InterruptedException {

        for (SinkRecord record : records) {

            // build batch key
            String formattedKeyPattern = httpSinkConfig.batchKeyPattern
                  .replace("${key}", record.key() == null ? "" : record.key().toString())
                  .replace("${topic}", record.topic());

            // add to batch and check for batch size limit
            if (!batches.containsKey(formattedKeyPattern)) {
                batches.put(formattedKeyPattern,
                      new ArrayList<SinkRecord>(Arrays.asList(new SinkRecord[]{record})));
            } else {
                batches.get(formattedKeyPattern).add(record);
            }
            if (batches.get(formattedKeyPattern).size() >= httpSinkConfig.batchMaxSize) {
                sendBatch(formattedKeyPattern);
            }
        }
        flushBatches();

    }

    public void flushBatches() throws IOException, ExecutionException, InterruptedException {
        // send any outstanding batches
        for (Map.Entry<String, List<SinkRecord>> entry : batches.entrySet()) {
            sendBatch(entry.getKey());
        }
    }

    private void sendBatch(String formattedKeyPattern)
          throws IOException, ExecutionException, InterruptedException {

        List<SinkRecord> records = batches.get(formattedKeyPattern);
        SinkRecord record = records.get(0);
        String recordKey = record.key() == null ? "" : record.key().toString();

        // build url - ${key} and ${topic} can be replaced with message values
        // the first record in the batch is used to build the url as we assume it will be consistent across all records.
        String formattedUrl = httpSinkConfig.httpApiUrl
              .replace("${key}", recordKey)
              .replace("${topic}", record.topic());
        HttpSinkConfig.RequestMethod requestMethod = httpSinkConfig.requestMethod;

        // add headers
        Map<String, String> headers = Arrays
              .stream(httpSinkConfig.headers.split(httpSinkConfig.headerSeparator))
              .filter((s) -> s.contains(":"))
              .map((s) -> s.split(":"))
              .collect(Collectors.toMap((s) -> s[0], (s) -> s[1]));

        String body = records.stream()
              .map(this::buildRecord)
              .collect(Collectors.joining(httpSinkConfig.batchSeparator, httpSinkConfig.batchPrefix,
                    httpSinkConfig.batchSuffix));

        log.debug("Submitting payload: {} to url: {}", body, formattedUrl);
        Response response = httpClient
              .makeRequest(requestMethod.toString(), formattedUrl, headers, body);

        //clear batch
        batches.remove(formattedKeyPattern);

        // handle failed response
        log.debug("Received Response: " + response.toString());
        if (!HttpUtil.isResponseSuccessful(response)) {
            throw new IOException(String.format(
                  "HTTP Response code: %s %s %s, Submitted payload: %s, url: %s",
                  response.getStatusCode(), response.getStatusMessage(), response.getBody(),
                  body, formattedUrl));
        }
        if (!httpSinkConfig.responseTopic.isEmpty()) {
            HttpResponse httpResponse = new HttpResponse(response.getStatusCode(), formattedUrl,
                  response.getStatusMessage(), response.getBody());
            kafkaClient.publish(recordKey, httpSinkConfig.responseTopic, httpResponse);
        }
    }

    private String buildRecord(SinkRecord record) {
        // add payload
        String value = record.value().toString();

        // apply regexes
        int replacementIndex = 0;
        for (String pattern : httpSinkConfig.regexPatterns.split(httpSinkConfig.regexSeparator)) {
            String replacement = "";
            if (replacementIndex < httpSinkConfig.regexReplacements
                  .split(httpSinkConfig.regexSeparator).length) {
                replacement = httpSinkConfig.regexReplacements
                      .split(httpSinkConfig.regexSeparator)[replacementIndex]
                      .replace("${key}", record.key() == null ? "" : record.key().toString())
                      .replace("${topic}", record.topic());
            }
            value = value.replaceAll(pattern, replacement);
            replacementIndex++;
        }
        return value;
    }

}
