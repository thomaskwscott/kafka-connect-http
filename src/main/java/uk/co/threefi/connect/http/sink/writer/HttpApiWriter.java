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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
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
import uk.co.threefi.connect.http.sink.dto.ResponseError;
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

    public Set<ResponseError> write(final Collection<SinkRecord> records)
          throws IOException, ExecutionException, InterruptedException, TimeoutException {

        final Set<ResponseError> responseErrors = new HashSet<>();

        for (final SinkRecord record : records) {
            // build batch key
            final String formattedKeyPattern = evaluateReplacements(httpSinkConfig.batchKeyPattern,
                  record);

            // add to batch and check for batch size limit
            if (!batches.containsKey(formattedKeyPattern)) {
                batches.put(formattedKeyPattern, new ArrayList<>(Collections.singletonList(record)));
            } else {
                batches.get(formattedKeyPattern).add(record);
            }

            if (batches.get(formattedKeyPattern).size() >= httpSinkConfig.batchMaxSize) {
                responseErrors.addAll(sendBatchAndGetResponseErrors(formattedKeyPattern));
            }
        }
        responseErrors.addAll(flushBatches());
        return responseErrors;
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
        final PayloadGenerator payloadGenerator){
        return new SalesforceAuthenticationProvider(
            httpSinkConfig.salesforceAuthenticationRoot,
            new JavaNetHttpClient(),
            Clock.systemDefaultZone(),
            payloadGenerator);
    }

    private PrivateKey extractPrivateKeyFromConfig(final HttpSinkConfig config)
          throws InvalidKeySpecException, NoSuchAlgorithmException {
        final byte[] keyBytes = Base64.getDecoder()
              .decode(config.salesforceAuthenticationPrivateKey
                    .replaceAll("-----BEGIN PRIVATE KEY-----", StringUtils.EMPTY)
                    .replaceAll("-----END PRIVATE KEY-----", StringUtils.EMPTY)
                    .replaceAll("\\s+", "")
                    .getBytes());
        final PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
        return KeyFactory.getInstance("RSA").generatePrivate(keySpec);
    }

    private Set<ResponseError> flushBatches() throws InterruptedException,
                                                     ExecutionException,
                                                     TimeoutException,
                                                     IOException {
        // send any outstanding batches
        final Set<ResponseError> responseErrors = new HashSet<>();
        for (Map.Entry<String, List<SinkRecord>> entry : batches.entrySet()) {
            responseErrors.addAll(sendBatchAndGetResponseErrors(entry.getKey()));
        }
        return responseErrors;
    }

    private Set<ResponseError> sendBatchAndGetResponseErrors(final String formattedKeyPattern)
          throws IOException, ExecutionException, InterruptedException, TimeoutException {

        final List<SinkRecord> records = batches.get(formattedKeyPattern);

        // the first record in the batch is used to build the url as we
        // assume it will be consistent across all records.
        final SinkRecord record = records.get(0);
        final String formattedUrl = evaluateReplacements(httpSinkConfig.httpApiUrl, record);
        final HttpSinkConfig.RequestMethod requestMethod = httpSinkConfig.requestMethod;

        // add headers
        final Map<String, String> headers = obtainHeaders();

        final String body = obtainBody(records);

        log.debug("Submitting payload: {} to url: {}", body, formattedUrl);

        final Response response = httpClient
              .makeRequest(requestMethod.toString(), formattedUrl, headers, body);

        //clear batch
        batches.remove(formattedKeyPattern);
        log.debug("Received Response: {}", response);
        // Uses first key of batch as key
        return responseHandler.processResponse(response, getKey(record), body, formattedUrl);
    }

    private String obtainBody(final List<SinkRecord> records){
        return records.stream()
            .map(this::buildRecord)
            .collect(Collectors.joining(httpSinkConfig.batchSeparator, httpSinkConfig.batchPrefix,
                httpSinkConfig.batchSuffix));
    }

    private Map<String, String> obtainHeaders(){
        return Arrays
            .stream(httpSinkConfig.headers.split(httpSinkConfig.headerSeparator))
            .filter(header -> header.contains(HEADER_VALUE_SEPARATOR))
            .map(header -> header.split(HEADER_VALUE_SEPARATOR))
            .collect(
                Collectors.toMap(splitHeader -> splitHeader[0], splitHeader -> splitHeader[1]));
    }

    private String buildRecord(final SinkRecord record) {
        String value = record.value() instanceof Struct
              ? buildJsonFromStruct((Struct) record.value(), httpSinkConfig.batchBodyFieldFilter)
              : record.value().toString();
        value = httpSinkConfig.batchBodyPrefix + value + httpSinkConfig.batchBodySuffix;

        int replacementIndex = 0;
        final String[] regexPatterns = httpSinkConfig.regexPatterns.split(httpSinkConfig.regexSeparator);
        for (String pattern : regexPatterns) {
            String replacement = StringUtils.EMPTY;
            final String[] regexReplacements = httpSinkConfig.regexReplacements
                  .split(httpSinkConfig.regexSeparator);
            if (replacementIndex < regexReplacements.length) {
                replacement = evaluateReplacements(regexReplacements[replacementIndex], record);
            }
            value = value.replaceAll(pattern, replacement);
            replacementIndex++;
        }
        return value;
    }

    private String evaluateReplacements(String inputString, SinkRecord record) {
        return inputString
              .replace("${key}", getKey(record))
              .replace("${topic}", record.topic());
    }
}
