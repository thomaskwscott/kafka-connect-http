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

import static org.assertj.core.api.Assertions.assertThat;
import static uk.co.threefi.connect.http.sink.RequestInfoAssert.assertThat;

import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import uk.co.threefi.connect.http.sink.MockKafkaAvroSerializer;
import uk.co.threefi.connect.http.sink.RequestInfo;
import uk.co.threefi.connect.http.sink.RestHelper;
import uk.co.threefi.connect.http.sink.client.ErrorKafkaClient;
import uk.co.threefi.connect.http.sink.client.ResponseKafkaClient;
import uk.co.threefi.connect.http.sink.config.HttpSinkConfig;
import uk.co.threefi.connect.http.sink.config.HttpSinkConfig.RequestMethod;
import uk.co.threefi.connect.http.sink.dto.ResponseError;
import uk.co.threefi.connect.http.sink.handler.ResponseHandler;

public class HttpApiWriterTest {

  private static final String PRIVATE_KEY =
      Base64.getEncoder()
          .encodeToString(Keys.keyPairFor(SignatureAlgorithm.RS256).getPrivate().getEncoded());
  private static final String SALESFORCE_LOGIN_URL = "/services/oauth2/token";
  private static final Pattern TOKEN_REQUEST_PATTERN =
      Pattern.compile(
          "^grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=.*$");
  private static final String DEFAULT_TOPIC = "user-topic";
  private final String endPoint = "/test";

  private final RestHelper restHelper = new RestHelper();

  @ClassRule
  public static final SharedKafkaTestResource kafkaTestHelper = new SharedKafkaTestResource();

  @Before
  public void setUp() throws Exception {
    restHelper.start();
  }

  @After
  public void tearDown() throws Exception {
    restHelper.stop();
    restHelper.flushCapturedRequests();
  }

  @Test
  public void putWithPayloadAndHeaders() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.PUT);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();

    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.PUT.toString())
        .hasUrl(endPoint)
        .hasBody((String) sinkRecords.get(0).value())
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void postWithPayloadAndHeaders() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody((String) sinkRecords.get(0).value())
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void deleteWithPayloadAndHeaders() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.DELETE);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.DELETE.toString())
        .hasUrl(endPoint)
        .hasBody((String) sinkRecords.get(0).value())
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void canCreateResponseErrorOnUnsuccessfulResponse() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.PUT);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    responseProducerProperties.put(
        HttpSinkConfig.HTTP_API_URL, "http://localhost:" + restHelper.getPort() + "/unauthorized");
    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    Set<ResponseError> responseErrors = writer.write(sinkRecords);
    assertThat(responseErrors).hasSize(1);
    assertThat(((ResponseError) responseErrors.toArray()[0]).getErrorMessage())
        .isEqualTo("{\"status\":\"unauthorized\"}");
  }

  @Test
  public void canSendResponseOverKafka() throws Exception {
    final String responseTopic = "response.topic";
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    responseProducerProperties.put(HttpSinkConfig.RESPONSE_TOPIC, responseTopic);
    responseProducerProperties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaTestHelper.getKafkaConnectString());

    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);

    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody((String) sinkRecords.get(0).value())
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");

    assertThat(kafkaTestHelper.getKafkaTestUtils().getTopics()).hasSize(1);
    assertThat(kafkaTestHelper.getKafkaTestUtils().getTopics().get(0).name())
        .isEqualTo(responseTopic);
    assertThat(kafkaTestHelper.getKafkaTestUtils().consumeAllRecordsFromTopic(responseTopic))
        .hasSize(1);
  }

  @Test
  public void multipleHeaders() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.DELETE);
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json|Cache-Control:no-cache");
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.DELETE.toString())
        .hasUrl(endPoint)
        .hasBody((String) sinkRecords.get(0).value())
        .hasHeaders(
            "Content-Type:application/json",
            "Authorization:Bearer aaa.bbb.ccc",
            "Cache-Control:no-cache");
  }

  @Test
  public void headerSeparator() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.DELETE);
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    responseProducerProperties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.DELETE.toString())
        .hasUrl(endPoint)
        .hasBody((String) sinkRecords.get(0).value())
        .hasHeaders(
            "Content-Type:application/json",
            "Authorization:Bearer aaa.bbb.ccc",
            "Cache-Control:no-cache");
  }

  @Test
  public void topicUrlSubstitution() throws Exception {
    String endPoint = "/${topic}";
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    responseProducerProperties.put(
        HttpSinkConfig.HTTP_API_URL, "http://localhost:" + restHelper.getPort() + endPoint);
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    responseProducerProperties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl("/someTopic")
        .hasBody((String) sinkRecords.get(0).value())
        .hasHeaders(
            "Content-Type:application/json",
            "Authorization:Bearer aaa.bbb.ccc",
            "Cache-Control:no-cache");
  }

  @Test
  public void keyUrlSubstitution() throws Exception {
    String endPoint = "/${key}";
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    responseProducerProperties.put(
        HttpSinkConfig.HTTP_API_URL, "http://localhost:" + restHelper.getPort() + endPoint);
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    responseProducerProperties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");

    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl("/someKey")
        .hasBody((String) sinkRecords.get(0).value())
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void multipleRecordsMultipleRequests() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    responseProducerProperties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");
    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(2);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 3);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody((String) sinkRecords.get(0).value())
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");

    assertThat(capturedRequests.get(2))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody((String) sinkRecords.get(1).value())
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void regexReplacement() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    responseProducerProperties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");
    responseProducerProperties.put(HttpSinkConfig.REGEX_PATTERNS, "^~$");
    responseProducerProperties.put(HttpSinkConfig.REGEX_REPLACEMENTS, "start~end");
    responseProducerProperties.put(HttpSinkConfig.REGEX_SEPARATOR, "~");

    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody("start" + sinkRecords.get(0).value() + "end")
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void regexReplacementWithKeyTopic() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    responseProducerProperties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");
    responseProducerProperties.put(HttpSinkConfig.REGEX_PATTERNS, "^~$");
    responseProducerProperties.put(HttpSinkConfig.REGEX_REPLACEMENTS, "${key}~${topic}");
    responseProducerProperties.put(HttpSinkConfig.REGEX_SEPARATOR, "~");

    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody("someKey" + sinkRecords.get(0).value() + "someTopic")
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void batchContainsPrefix() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    responseProducerProperties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");
    responseProducerProperties.put(HttpSinkConfig.REGEX_PATTERNS, "^~$");
    responseProducerProperties.put(HttpSinkConfig.REGEX_REPLACEMENTS, "${key}~${topic}");
    responseProducerProperties.put(HttpSinkConfig.REGEX_SEPARATOR, "~");
    responseProducerProperties.put(HttpSinkConfig.BATCH_PREFIX, "batchPrefix");

    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody("batchPrefixsomeKeysomeValue1someTopic")
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void batchContainsSuffix() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    responseProducerProperties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");
    responseProducerProperties.put(HttpSinkConfig.REGEX_PATTERNS, "^~$");
    responseProducerProperties.put(HttpSinkConfig.REGEX_REPLACEMENTS, "${key}~${topic}");
    responseProducerProperties.put(HttpSinkConfig.REGEX_SEPARATOR, "~");
    responseProducerProperties.put(HttpSinkConfig.BATCH_SUFFIX, "batchSuffix");

    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody("someKeysomeValue1someTopicbatchSuffix")
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void batchSentAtMaxSize() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    responseProducerProperties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");
    responseProducerProperties.put(HttpSinkConfig.REGEX_PATTERNS, "^~$");
    responseProducerProperties.put(HttpSinkConfig.REGEX_REPLACEMENTS, "${key}~${topic}");
    responseProducerProperties.put(HttpSinkConfig.REGEX_SEPARATOR, "~");
    responseProducerProperties.put(HttpSinkConfig.BATCH_MAX_SIZE, "2");

    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(2);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody("someKeysomeValue1someTopic,someKeysomeValue2someTopic")
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void batchesSplitByKeyPattern() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    responseProducerProperties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");
    responseProducerProperties.put(HttpSinkConfig.REGEX_PATTERNS, "^~$");
    responseProducerProperties.put(HttpSinkConfig.REGEX_REPLACEMENTS, "${key}~${topic}");
    responseProducerProperties.put(HttpSinkConfig.REGEX_SEPARATOR, "~");
    responseProducerProperties.put(HttpSinkConfig.BATCH_KEY_PATTERN, "${topic}-${key}");

    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue";
    sinkRecords.add(new SinkRecord("someTopic1", 0, null, "someKey1", null, payload1, 0));
    sinkRecords.add(new SinkRecord("someTopic2", 0, null, "someKey2", null, payload1, 0));

    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 3);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody("someKey1someValuesomeTopic1")
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");

    assertThat(capturedRequests.get(2))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody("someKey2someValuesomeTopic2")
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void batchesSplitByConstantKeyPattern() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    responseProducerProperties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");
    responseProducerProperties.put(HttpSinkConfig.REGEX_PATTERNS, "^~$");
    responseProducerProperties.put(HttpSinkConfig.REGEX_REPLACEMENTS, "${key}~${topic}");
    responseProducerProperties.put(HttpSinkConfig.REGEX_SEPARATOR, "~");
    responseProducerProperties.put(HttpSinkConfig.BATCH_MAX_SIZE, "2");
    responseProducerProperties.put(HttpSinkConfig.BATCH_KEY_PATTERN, "someKey");

    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue";
    sinkRecords.add(new SinkRecord("someTopic1", 0, null, "someKey1", null, payload1, 0));
    sinkRecords.add(new SinkRecord("someTopic2", 0, null, "someKey2", null, payload1, 0));

    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody("someKey1someValuesomeTopic1,someKey2someValuesomeTopic2")
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void multipleBatchesSentAtMaxSize() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    responseProducerProperties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");
    responseProducerProperties.put(HttpSinkConfig.REGEX_PATTERNS, "^~$");
    responseProducerProperties.put(HttpSinkConfig.REGEX_REPLACEMENTS, "${key}~${topic}");
    responseProducerProperties.put(HttpSinkConfig.REGEX_SEPARATOR, "~");
    responseProducerProperties.put(HttpSinkConfig.BATCH_MAX_SIZE, "2");
    responseProducerProperties.put(HttpSinkConfig.BATCH_KEY_PATTERN, "${topic}");

    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue";

    sinkRecords.add(new SinkRecord("someTopic1", 0, null, "someKey1", null, payload1, 0));
    sinkRecords.add(new SinkRecord("someTopic2", 0, null, "someKey2", null, payload1, 0));
    sinkRecords.add(new SinkRecord("someTopic1", 0, null, "someKey2", null, payload1, 0));
    sinkRecords.add(new SinkRecord("someTopic2", 0, null, "someKey1", null, payload1, 0));

    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 3);

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody("someKey1someValuesomeTopic1,someKey2someValuesomeTopic1")
        .hasHeaders(
            "Content-Type:application/json",
            "Authorization:Bearer aaa.bbb.ccc",
            "Cache-Control:no-cache");
    assertThat(capturedRequests.get(2))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody("someKey2someValuesomeTopic2,someKey1someValuesomeTopic2")
        .hasHeaders(
            "Content-Type:application/json",
            "Authorization:Bearer aaa.bbb.ccc",
            "Cache-Control:no-cache");
  }

  @Test
  public void testStructValue() throws Exception {

    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    responseProducerProperties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");
    responseProducerProperties.put(HttpSinkConfig.REGEX_PATTERNS, "^~$");
    responseProducerProperties.put(HttpSinkConfig.REGEX_REPLACEMENTS, "${key}~${topic}");
    responseProducerProperties.put(HttpSinkConfig.REGEX_SEPARATOR, "~");
    responseProducerProperties.put(HttpSinkConfig.BATCH_MAX_SIZE, "2");
    responseProducerProperties.put(HttpSinkConfig.BATCH_KEY_PATTERN, "${topic}");

    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = new ArrayList<>();

    Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    Struct structData = new Struct(valueSchema).put("id", "fake-user-id").put("name", null);

    sinkRecords.add(
        new SinkRecord(DEFAULT_TOPIC, 0, null, "fake-user-id", valueSchema, structData, 0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody("fake-user-id{\"id\":\"fake-user-id\"}" + DEFAULT_TOPIC)
        .hasHeaders(
            "Content-Type:application/json",
            "Authorization:Bearer aaa.bbb.ccc",
            "Cache-Control:no-cache");
  }

  @Test
  public void testStructValueRemoveList() throws Exception {

    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    responseProducerProperties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");
    responseProducerProperties.put(HttpSinkConfig.REGEX_PATTERNS, "^~$");
    responseProducerProperties.put(HttpSinkConfig.REGEX_REPLACEMENTS, "${key}~${topic}");
    responseProducerProperties.put(HttpSinkConfig.REGEX_SEPARATOR, "~");
    responseProducerProperties.put(HttpSinkConfig.BATCH_MAX_SIZE, "2");
    responseProducerProperties.put(HttpSinkConfig.BATCH_KEY_PATTERN, "${topic}");
    responseProducerProperties.put(HttpSinkConfig.BATCH_BODY_FIELD_FILTER, "id,test");

    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = new ArrayList<>();

    Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("test", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    Struct structData =
        new Struct(valueSchema)
            .put("id", "fake-user-id")
            .put("name", "John Smith")
            .put("test", "To be removed");

    sinkRecords.add(
        new SinkRecord(DEFAULT_TOPIC, 0, null, "fake-user-id", valueSchema, structData, 0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody("fake-user-id{\"name\":\"John Smith\"}" + DEFAULT_TOPIC)
        .hasHeaders(
            "Content-Type:application/json",
            "Authorization:Bearer aaa.bbb.ccc",
            "Cache-Control:no-cache");
  }

  @Test
  public void testStructValueUuidRemovalNoField() throws Exception {

    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    responseProducerProperties.put(
        HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    responseProducerProperties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");
    responseProducerProperties.put(HttpSinkConfig.REGEX_PATTERNS, "^~$");
    responseProducerProperties.put(HttpSinkConfig.REGEX_REPLACEMENTS, "${key}~${topic}");
    responseProducerProperties.put(HttpSinkConfig.REGEX_SEPARATOR, "~");
    responseProducerProperties.put(HttpSinkConfig.BATCH_MAX_SIZE, "2");
    responseProducerProperties.put(HttpSinkConfig.BATCH_KEY_PATTERN, "${topic}");
    responseProducerProperties.put(HttpSinkConfig.BATCH_BODY_FIELD_FILTER, "some-other-field");

    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = new ArrayList<>();

    Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    Struct structData = new Struct(valueSchema).put("id", "fake-user-id").put("name", "John Smith");

    sinkRecords.add(
        new SinkRecord(DEFAULT_TOPIC, 0, null, "fake-user-id", valueSchema, structData, 0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody("fake-user-id{\"id\":\"fake-user-id\",\"name\":\"John Smith\"}" + DEFAULT_TOPIC)
        .hasHeaders(
            "Content-Type:application/json",
            "Authorization:Bearer aaa.bbb.ccc",
            "Cache-Control:no-cache");
  }

  @Test
  public void canProcessMultipleKeysAndRecords() throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    responseProducerProperties.put("batch.max.size", "4");
    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(6);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 3);

    String firstBody =
        sinkRecords.stream()
            .filter(sinkRecord -> sinkRecord.value().toString().matches(".+[1-4]"))
            .map(sinkRecord -> sinkRecord.value().toString())
            .collect(Collectors.joining(","));

    String secondBody =
        sinkRecords.stream()
            .filter(sinkRecord -> sinkRecord.value().toString().matches(".+[5-6]"))
            .map(sinkRecord -> sinkRecord.value().toString())
            .collect(Collectors.joining(","));

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody(firstBody)
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");

    assertThat(capturedRequests.get(2))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody(secondBody)
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void canAddBatchBodyPrefix() throws Exception {
    String bodyPrefix = "\"method\" : \"PATCH\",";
    canAddBatchBodyAffix(HttpSinkConfig.BATCH_BODY_PREFIX, bodyPrefix);
  }

  @Test
  public void canAddBatchSuffix() throws Exception {
    String bodySuffix = ", \"referenceId\" : \"334234\"";
    canAddBatchBodyAffix(HttpSinkConfig.BATCH_BODY_SUFFIX, bodySuffix);
  }

  private void canAddBatchBodyAffix(String configName, String affix) throws Exception {
    Map<String, String> responseProducerProperties =
        getResponseProducerProperties(RequestMethod.POST);
    responseProducerProperties.put(configName, affix);
    Map<String, String> errorProducerProperties = getErrorProducerProperties();
    HttpApiWriter writer = getHttpApiWriter(responseProducerProperties, errorProducerProperties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    String body =
        responseProducerProperties.containsKey(HttpSinkConfig.BATCH_BODY_PREFIX)
            ? affix + sinkRecords.get(0).value()
            : sinkRecords.get(0).value() + affix;

    assertThat(capturedRequests.get(1))
        .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
        .hasUrl(endPoint)
        .hasBody(body)
        .hasHeaders("Content-Type:application/json", "Authorization:Bearer aaa.bbb.ccc");
  }

  private Map<String, String> getResponseProducerProperties(RequestMethod requestMethod) {
    Map<String, String> responseProducerProperties = getCommonProperties(requestMethod);
    responseProducerProperties.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    responseProducerProperties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MockKafkaAvroSerializer.class.getName());
    return responseProducerProperties;
  }

  private Map<String, String> getErrorProducerProperties() {
    Map<String, String> errorProducerProperties = getCommonProperties(RequestMethod.POST);
    errorProducerProperties.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    errorProducerProperties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return errorProducerProperties;
  }

  private Map<String, String> getCommonProperties(RequestMethod requestMethod) {
    int port = restHelper.getPort();
    String testUrl = "http://localhost:" + port + endPoint;

    Map<String, String> properties = new HashMap<>();
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD, requestMethod.toString());
    properties.put(HttpSinkConfig.HEADERS, "Content-Type:application/json");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(
        HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT,
        String.format("http://localhost:%s", restHelper.getPort()));

    properties.put(ProducerConfig.RETRIES_CONFIG, "1");
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
    properties.put("schema.registry.url", "http://localhost:8081");
    return properties;
  }

  private List<SinkRecord> createSinkRecords(int records) {
    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (int i = 1; i <= records; i++) {
      String payload = "someValue" + i;
      sinkRecords.add(new SinkRecord("someTopic", 0, null, "someKey", null, payload, 0));
    }
    return sinkRecords;
  }

  private HttpApiWriter getHttpApiWriter(
      Map<String, String> responseProducerProperties, Map<String, String> errorProducerProperties)
      throws Exception {

    HttpSinkConfig httpSinkConfig = new HttpSinkConfig(responseProducerProperties);
    ProducerConfig responseProducerConfig =
        new ProducerConfig(Collections.unmodifiableMap(responseProducerProperties));
    ProducerConfig errorProducerConfig =
        new ProducerConfig(Collections.unmodifiableMap(errorProducerProperties));

    final ResponseKafkaClient responseKafkaClient = new ResponseKafkaClient(responseProducerConfig);
    final ErrorKafkaClient errorKafkaClient = new ErrorKafkaClient(errorProducerConfig);

    ResponseHandler responseHandler =
        new ResponseHandler(httpSinkConfig, responseKafkaClient, errorKafkaClient);

    return new HttpApiWriter(responseHandler);
  }

  private void commonAssert(List<RequestInfo> capturedRequests, int capturedRequestSize) {
    assertThat(capturedRequests).hasSize(capturedRequestSize);
    assertThat(capturedRequests.get(0))
        .hasMethod(RequestMethod.POST.toString())
        .hasUrl(SALESFORCE_LOGIN_URL)
        .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);
  }
}
