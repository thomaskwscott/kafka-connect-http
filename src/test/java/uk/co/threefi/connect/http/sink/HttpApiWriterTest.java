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

import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.util.Base64;
import java.util.regex.Pattern;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.co.threefi.connect.http.Assertions.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;


public class HttpApiWriterTest {
  private static final String PRIVATE_KEY = Base64.getEncoder()
          .encodeToString(Keys.keyPairFor(SignatureAlgorithm.RS256).getPrivate().getEncoded());
  private static final String SALESFORCE_LOGIN_URL = "/services/oauth2/token";
  private final RestHelper restHelper = new RestHelper();
  private static final Pattern TOKEN_REQUEST_PATTERN =
          Pattern.compile("^grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=.*$");

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
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/test";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.PUT.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload = "someValue";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload,0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(2);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.PUT.toString())
            .hasUrl(endPoint)
            .hasBody(payload)
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void postWithPayloadAndHeaders() throws Exception {
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/test";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.POST.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload = "someValue";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload,0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(2);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody(payload)
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void deleteWithPayloadAndHeaders() throws Exception {
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/test";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.DELETE.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload = "someValue";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload,0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(2);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.DELETE.toString())
            .hasUrl(endPoint)
            .hasBody(payload)
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void multipleHeaders() throws Exception {
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/test";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.DELETE.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json|Cache-Control:no-cache");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload = "someValue";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload,0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(2);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.DELETE.toString())
            .hasUrl(endPoint)
            .hasBody(payload)
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc",
                    "Cache-Control:no-cache");
  }

  @Test
  public void headerSeparator() throws Exception {
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/test";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.DELETE.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload = "someValue";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload,0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(2);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.DELETE.toString())
            .hasUrl(endPoint)
            .hasBody(payload)
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc",
                    "Cache-Control:no-cache");
  }

  @Test
  public void topicUrlSubstitution() throws Exception {
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/${topic}";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.POST.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload = "someValue";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload,0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(2);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl("/someTopic")
            .hasBody(payload)
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc",
                    "Cache-Control:no-cache");
  }

  @Test
  public void keyUrlSubstitution() throws Exception {
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/${key}";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.POST.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload = "someValue";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload,0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(2);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl("/someKey")
            .hasBody(payload)
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void multipleRecordsMultipleRequests() throws Exception {
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/test";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.POST.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue1";
    String payload2 = "someValue2";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload2,0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(3);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody(payload1)
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");

    assertThat(capturedRequests.get(2))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody(payload2)
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void regexReplacement() throws Exception {
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/test";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.POST.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"start~end");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue1";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload1,0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(2);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody("start" + payload1 + "end")
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void regexReplacementWithKeyTopic() throws Exception {
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/test";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.POST.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"${key}~${topic}");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue1";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload1,0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(2);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

    assertThat(capturedRequests.get(1))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(endPoint)
            .hasBody("someKey" + payload1 + "someTopic")
            .hasHeaders(
                    "Content-Type:application/json",
                    "Authorization:Bearer aaa.bbb.ccc");
  }

  @Test
  public void batchContainsPrefix() throws Exception {
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/test";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.POST.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"${key}~${topic}");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");
    properties.put(HttpSinkConfig.BATCH_PREFIX,"batchPrefix");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue1";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload1,0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(2);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

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
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/test";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.POST.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"${key}~${topic}");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");
    properties.put(HttpSinkConfig.BATCH_SUFFIX,"batchSuffix");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue1";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload1,0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(2);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

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
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/test";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.POST.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"${key}~${topic}");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");
    properties.put(HttpSinkConfig.BATCH_MAX_SIZE,"2");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue1";
    String payload2 = "someValue2";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload2,0));
    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(2);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

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
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/test";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.POST.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"${key}~${topic}");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");
    properties.put(HttpSinkConfig.BATCH_KEY_PATTERN,"${topic}-${key}");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue";
    sinkRecords.add(new SinkRecord("someTopic1",0,null,"someKey1",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic2",0,null,"someKey2",null, payload1,0));

    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(3);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

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
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/test";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.POST.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"${key}~${topic}");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");
    properties.put(HttpSinkConfig.BATCH_MAX_SIZE,"2");
    properties.put(HttpSinkConfig.BATCH_KEY_PATTERN,"someKey");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue";
    sinkRecords.add(new SinkRecord("someTopic1",0,null,"someKey1",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic2",0,null,"someKey2",null, payload1,0));

    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(2);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

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
    Map<String,String> properties = new HashMap<>();
    int port = restHelper.getPort();
    String endPoint = "/test";
    String testUrl = "http://localhost:" + port + endPoint;
    properties.put(HttpSinkConfig.HTTP_API_URL, testUrl);
    properties.put(HttpSinkConfig.REQUEST_METHOD,HttpSinkConfig.RequestMethod.POST.toString());
    properties.put(HttpSinkConfig.HEADERS,"Content-Type:application/json=Cache-Control:no-cache");
    properties.put(HttpSinkConfig.HEADER_SEPERATOR,"=");
    properties.put(HttpSinkConfig.REGEX_PATTERNS,"^~$");
    properties.put(HttpSinkConfig.REGEX_REPLACEMENTS,"${key}~${topic}");
    properties.put(HttpSinkConfig.REGEX_SEPARATOR,"~");
    properties.put(HttpSinkConfig.BATCH_MAX_SIZE,"2");
    properties.put(HttpSinkConfig.BATCH_KEY_PATTERN,"${topic}");
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_PRIVATE_KEY, PRIVATE_KEY);
    properties.put(HttpSinkConfig.SALESFORCE_AUTHENTICATION_ROOT, String.format("http://localhost:%s", restHelper.getPort()));


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue";


    sinkRecords.add(new SinkRecord("someTopic1",0,null,"someKey1",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic2",0,null,"someKey2",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic1",0,null,"someKey2",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic2",0,null,"someKey1",null, payload1,0));



    writer.write(sinkRecords);

    List<RequestInfo> capturedRequests = restHelper.getCapturedRequests();
    assertThat(capturedRequests).hasSize(3);
    assertThat(capturedRequests.get(0))
            .hasMethod(HttpSinkConfig.RequestMethod.POST.toString())
            .hasUrl(SALESFORCE_LOGIN_URL)
            .hasHeaders("Content-Type:application/x-www-form-urlencoded");
    assertThat(capturedRequests.get(0).getBody()).matches(TOKEN_REQUEST_PATTERN);

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


}