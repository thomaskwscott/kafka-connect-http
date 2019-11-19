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

package uk.co.threefi.connect.http.sink.writer;

import static uk.co.threefi.connect.http.util.DataUtils.buildJsonFromStruct;
import static uk.co.threefi.connect.http.util.DataUtils.getKey;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.threefi.connect.http.sink.client.AuthenticatedJavaNetHttpClient;
import uk.co.threefi.connect.http.sink.client.JavaNetHttpClient;
import uk.co.threefi.connect.http.sink.config.HttpSinkConfig;
import uk.co.threefi.connect.http.sink.dto.Response;
import uk.co.threefi.connect.http.sink.dto.RetriableError;
import uk.co.threefi.connect.http.sink.generator.PayloadGenerator;
import uk.co.threefi.connect.http.sink.handler.ResponseHandler;
import uk.co.threefi.connect.http.sink.provider.SalesforceAuthenticationProvider;

public class HttpApiWriter {

  private static final String HEADER_VALUE_SEPARATOR = ":";

  private final JavaNetHttpClient httpClient;
  private final HttpSinkConfig httpSinkConfig;
  private static final Logger log = LoggerFactory.getLogger(HttpApiWriter.class);
  private Map<String, List<SinkRecord>> batches = new HashMap<>();
  private final ResponseHandler responseHandler;

  public HttpApiWriter(final ResponseHandler responseHandler) throws Exception {
    this.httpSinkConfig = responseHandler.getHttpSinkConfig();
    this.responseHandler = responseHandler;
    httpClient = obtainAuthenticatedJavaNetHttpClient();
  }

  public Set<RetriableError> write(final Collection<SinkRecord> records) {

    final Set<RetriableError> retriableErrors = new HashSet<>();
    for (SinkRecord record : records) {
      // build batch key
      final String formattedKeyPattern =
          evaluateReplacements(httpSinkConfig.batchKeyPattern, record);

      // add to batch and check for batch size limit
      if (!batches.containsKey(formattedKeyPattern)) {
        batches.put(formattedKeyPattern, new ArrayList<>(Collections.singletonList(record)));
      } else {
        batches.get(formattedKeyPattern).add(record);
      }

      if (batches.get(formattedKeyPattern).size() >= httpSinkConfig.batchMaxSize) {
        retriableErrors.addAll(sendBatchAndGetRetriableErrors(formattedKeyPattern));
      }
    }
    retriableErrors.addAll(flushBatches());
    return retriableErrors;
  }

  private AuthenticatedJavaNetHttpClient obtainAuthenticatedJavaNetHttpClient()
      throws InvalidKeySpecException, NoSuchAlgorithmException {

    final PayloadGenerator payloadGenerator = obtainPayloadGenerator();
    final SalesforceAuthenticationProvider authenticationProvider =
        obtainSalesforceAuthenticationProvider(payloadGenerator);

    return new AuthenticatedJavaNetHttpClient(authenticationProvider);
  }

  private PayloadGenerator obtainPayloadGenerator()
      throws InvalidKeySpecException, NoSuchAlgorithmException {

    return new PayloadGenerator(
        extractPrivateKeyFromConfig(httpSinkConfig),
        httpSinkConfig.salesforceAuthenticationClientId,
        httpSinkConfig.salesforceAuthenticationUsername,
        httpSinkConfig.salesforceAuthenticationRoot);
  }

  private SalesforceAuthenticationProvider obtainSalesforceAuthenticationProvider(
      final PayloadGenerator payloadGenerator) {
    return new SalesforceAuthenticationProvider(
        httpSinkConfig.salesforceAuthenticationRoot,
        new JavaNetHttpClient(),
        Clock.systemDefaultZone(),
        payloadGenerator);
  }

  private PrivateKey extractPrivateKeyFromConfig(final HttpSinkConfig config)
      throws InvalidKeySpecException, NoSuchAlgorithmException {
    final byte[] keyBytes =
        Base64.getDecoder()
            .decode(
                config
                    .salesforceAuthenticationPrivateKey
                    .replaceAll("-----BEGIN PRIVATE KEY-----", StringUtils.EMPTY)
                    .replaceAll("-----END PRIVATE KEY-----", StringUtils.EMPTY)
                    .replaceAll("\\s+", "")
                    .getBytes());
    final PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
    return KeyFactory.getInstance("RSA").generatePrivate(keySpec);
  }

  private Set<RetriableError> flushBatches() {
    // send any outstanding batches
    Set<RetriableError> retriableErrors = new HashSet<>();
    for (Map.Entry<String, List<SinkRecord>> entry : batches.entrySet()) {
      retriableErrors.addAll(sendBatchAndGetRetriableErrors(entry.getKey()));
    }
    return retriableErrors;
  }

  private Set<RetriableError> sendBatchAndGetRetriableErrors(String formattedKeyPattern) {
    List<SinkRecord> records = batches.get(formattedKeyPattern);
    try {

      // the first record in the batch is used to build the url as we
      // assume it will be consistent across all records.
      SinkRecord record = records.get(0);
      String formattedUrl = evaluateReplacements(httpSinkConfig.httpApiUrl, record);
      HttpSinkConfig.RequestMethod requestMethod = httpSinkConfig.requestMethod;

      // add headers
      final Map<String, String> headers = obtainHeaders();

      final String body = obtainBody(records);

      log.debug("Submitting payload: {} to url: {}", body, formattedUrl);
      final Response response =
          httpClient.makeRequest(requestMethod.toString(), formattedUrl, headers, body);

      batches.remove(formattedKeyPattern);
      log.debug("Received Response: {}", response);
      // Uses first key of batch as key
      return responseHandler.processResponse(response, getKey(record), body, formattedUrl);
    } catch (Exception exception) {
      return records.stream()
          .map(sinkRecord -> new RetriableError(sinkRecord, exception.getMessage()))
          .collect(Collectors.toSet());
    }
  }

  private String obtainBody(final List<SinkRecord> records) {
    return records.stream()
        .map(this::buildRecord)
        .collect(
            Collectors.joining(
                httpSinkConfig.batchSeparator,
                httpSinkConfig.batchPrefix,
                httpSinkConfig.batchSuffix));
  }

  private Map<String, String> obtainHeaders() {
    return Arrays.stream(httpSinkConfig.headers.split(httpSinkConfig.headerSeparator))
        .filter(header -> header.contains(HEADER_VALUE_SEPARATOR))
        .map(header -> header.split(HEADER_VALUE_SEPARATOR))
        .collect(Collectors.toMap(splitHeader -> splitHeader[0], splitHeader -> splitHeader[1]));
  }

  private String buildRecord(final SinkRecord record) {
    String value =
        record.value() instanceof Struct
            ? buildJsonFromStruct((Struct) record.value(), httpSinkConfig.batchBodyFieldFilter)
            : record.value().toString();
    value = httpSinkConfig.batchBodyPrefix + value + httpSinkConfig.batchBodySuffix;

    int replacementIndex = 0;
    String[] regexPatterns = httpSinkConfig.regexPatterns.split(httpSinkConfig.regexSeparator);
    for (String pattern : regexPatterns) {
      String replacement = StringUtils.EMPTY;
      String[] regexReplacements =
          httpSinkConfig.regexReplacements.split(httpSinkConfig.regexSeparator);
      if (replacementIndex < regexReplacements.length) {
        replacement = evaluateReplacements(regexReplacements[replacementIndex], record);
      }
      value = value.replaceAll(pattern, replacement);
      replacementIndex++;
    }
    return value;
  }

  private String evaluateReplacements(String inputString, SinkRecord record) {
    return inputString.replace("${key}", getKey(record)).replace("${topic}", record.topic());
  }
}
