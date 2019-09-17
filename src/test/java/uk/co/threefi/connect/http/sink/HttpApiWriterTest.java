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

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.threefi.connect.http.sink.HttpSinkConfig.RequestMethod;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.co.threefi.connect.http.sink.RequestInfoAssert.assertThat;

public class HttpApiWriterTest {
  private static final String PRIVATE_KEY = Base64.getEncoder()
          .encodeToString(Keys.keyPairFor(SignatureAlgorithm.RS256).getPrivate().getEncoded());
  private static final String SALESFORCE_LOGIN_URL = "/services/oauth2/token";
  private final RestHelper restHelper = new RestHelper();
  private static final Pattern TOKEN_REQUEST_PATTERN =
          Pattern.compile("^grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=.*$");
  
  private final String endPoint = "/test";
  
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
    Map<String,String> properties = getProperties(RequestMethod.PUT);

    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.PUT.toString())
            .hasUrl(endPoint)
            .hasBody((String)sinkRecords.get(0).value())
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void postWithPayloadAndHeaders() throws Exception {
    Map<String,String> properties = getProperties(RequestMethod.POST);
    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody((String)sinkRecords.get(0).value())
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void deleteWithPayloadAndHeaders() throws Exception {
    Map<String,String> properties = getProperties(RequestMethod.DELETE);

    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.DELETE.toString())
            .hasUrl(endPoint)
            .hasBody((String)sinkRecords.get(0).value())
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void multipleHeaders() throws Exception {
    Map<String,String> properties = getProperties(RequestMethod.DELETE);
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json|Cache-Control:no-cache");
    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.DELETE.toString())
            .hasUrl(endPoint)
            .hasBody((String)sinkRecords.get(0).value())
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc",
                    "Cache-Control:no-cache");
  }

  @Test
  public void headerSeparator() throws Exception {
    Map<String,String> properties = getProperties(RequestMethod.DELETE);
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");

    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.DELETE.toString())
            .hasUrl(endPoint)
            .hasBody((String)sinkRecords.get(0).value())
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc",
                    "Cache-Control:no-cache");
  }

  @Test
  public void topicUrlSubstitution() throws Exception {
    String endPoint = "/${topic}";
    Map<String, String> properties = getProperties(RequestMethod.POST);
    properties.put(HttpSinkConfig.HTTP_API_URL,
          "http://localhost:" + restHelper.getPort() + endPoint);
    properties.put(HttpSinkConfig.HEADERS, "Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR, "=");

    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl("/someTopic")
            .hasBody((String)sinkRecords.get(0).value())
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc",
                    "Cache-Control:no-cache");
  }

  @Test
  public void keyUrlSubstitution() throws Exception {
    String endPoint = "/${key}";
    Map<String,String> properties = getProperties(RequestMethod.POST);
    properties.put(HttpSinkConfig.HTTP_API_URL,
          "http://localhost:" + restHelper.getPort() + endPoint);
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");

    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl("/someKey")
            .hasBody((String)sinkRecords.get(0).value())
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void multipleRecordsMultipleRequests() throws Exception {
    Map<String,String> properties = getProperties(RequestMethod.POST);
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = createSinkRecords(2);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 3);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody((String)sinkRecords.get(0).value())
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");

    assertThat(capturedRequests.get(2))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody((String)sinkRecords.get(1).value())
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void regexReplacement() throws Exception {
    Map<String,String> properties = getProperties(RequestMethod.POST);
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"start~end");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");

    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody("start" + sinkRecords.get(0).value() + "end")
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void regexReplacementWithKeyTopic() throws Exception {
    Map<String,String> properties = getProperties(RequestMethod.POST);
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"${key}~${topic}");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");

    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody("someKey" + sinkRecords.get(0).value() + "someTopic")
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void batchContainsPrefix() throws Exception {
    Map<String,String> properties = getProperties(RequestMethod.POST);
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"${key}~${topic}");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");
    properties.put(HttpSinkConfig.BATCH_PREFIX,"batchPrefix");

    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody("batchPrefixsomeKeysomeValue1someTopic")
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void batchContainsSuffix() throws Exception {
    Map<String,String> properties = getProperties(RequestMethod.POST);
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"${key}~${topic}");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");
    properties.put(HttpSinkConfig.BATCH_SUFFIX,"batchSuffix");

    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = createSinkRecords(1);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody("someKeysomeValue1someTopicbatchSuffix")
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void batchSentAtMaxSize() throws Exception {
    Map<String,String> properties = getProperties(RequestMethod.POST);
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"${key}~${topic}");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");
    properties.put(HttpSinkConfig.BATCH_MAX_SIZE,"2");

    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = createSinkRecords(2);
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody("someKeysomeValue1someTopic,someKeysomeValue2someTopic")
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void batchesSplitByKeyPattern() throws Exception {
    Map<String,String> properties = getProperties(RequestMethod.POST);
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"${key}~${topic}");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");
    properties.put(HttpSinkConfig.BATCH_KEY_PATTERN,"${topic}-${key}");

    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue";
    sinkRecords.add(new SinkRecord("someTopic1",0,null,"someKey1",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic2",0,null,"someKey2",null, payload1,0));

    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 3);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody("someKey1someValuesomeTopic1")
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");

    assertThat(capturedRequests.get(2))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody("someKey2someValuesomeTopic2")
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void batchesSplitByConstantKeyPattern() throws Exception {
    Map<String,String> properties = getProperties(RequestMethod.POST);
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"${key}~${topic}");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");
    properties.put(HttpSinkConfig.BATCH_MAX_SIZE,"2");
    properties.put(HttpSinkConfig.BATCH_KEY_PATTERN,"someKey");

    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue";
    sinkRecords.add(new SinkRecord("someTopic1",0,null,"someKey1",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic2",0,null,"someKey2",null, payload1,0));

    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    commonAssert(capturedRequests, 2);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody("someKey1someValuesomeTopic1,someKey2someValuesomeTopic2")
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void multipleBatchesSentAtMaxSize() throws Exception {
    Map<String,String> properties = getProperties(RequestMethod.POST);
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"${key}~${topic}");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");
    properties.put(HttpSinkConfig.BATCH_MAX_SIZE,"2");
    properties.put(HttpSinkConfig.BATCH_KEY_PATTERN,"${topic}");

    HttpApiWriter writer = getHttpApiWriter(properties);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue";

    sinkRecords.add(new SinkRecord("someTopic1",0,null,"someKey1",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic2",0,null,"someKey2",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic1",0,null,"someKey2",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic2",0,null,"someKey1",null, payload1,0));

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

  private Map<String, String> getProperties(RequestMethod requestMethod) {
    int port = restHelper.getPort();
    String testUrl = "http://localhost:" + port + endPoint;

    Map<String, String> properties = new HashMap<>();
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD, requestMethod.toString());
    properties.put(HttpSinkConfig.HEADERS, "Content-Type:application/json");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT,
          String.format("http://localhost:%s", restHelper.getPort()));

    properties.put(ProducerConfig.RETRIES_CONFIG, "1");
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer");
    properties
          .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    properties.put("schema.registry.url","http://localhost:8081");
    return properties;
  }

  private List<SinkRecord> createSinkRecords(int records) {
    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (int i = 1; i <= records; i++) {
      String payload = "someValue" + i;
      sinkRecords.add(new SinkRecord(
            "someTopic", 0, null, "someKey",
            null, payload, 0));
    }
    return sinkRecords;
  }

  private HttpApiWriter getHttpApiWriter(Map<String, String> properties) throws Exception {
    HttpSinkConfig config = new HttpSinkConfig(properties);
    ProducerConfig producerConfig = new ProducerConfig(Collections.unmodifiableMap(properties));
    return new HttpApiWriter(config, producerConfig);
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