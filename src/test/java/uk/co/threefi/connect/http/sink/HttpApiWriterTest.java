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

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.IsCollectionContaining.hasItems;


public class HttpApiWriterTest {

  private final RestHelper restHelper = new RestHelper();

  private HttpApiWriter writer = null;

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
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload = "someValue";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload,0));
    writer.write(sinkRecords);
    Assert.assertEquals(1,restHelper.getCapturedRequests().size());
    for( RequestInfo requestInfo : restHelper.getCapturedRequests())
    {
      Assert.assertEquals(HttpSinkConfig.RequestMethod.PUT.toString(),requestInfo.getMethod());
      Assert.assertEquals(endPoint,requestInfo.getUrl());
      Assert.assertEquals(payload,requestInfo.getBody());
      Assert.assertThat(requestInfo.getHeaders(),hasItems("Content-Type:application/json"));
    }
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
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload = "someValue";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload,0));
    writer.write(sinkRecords);
    Assert.assertEquals(1,restHelper.getCapturedRequests().size());
    for( RequestInfo requestInfo : restHelper.getCapturedRequests())
    {
      Assert.assertEquals(HttpSinkConfig.RequestMethod.POST.toString(),requestInfo.getMethod());
      Assert.assertEquals(endPoint,requestInfo.getUrl());
      Assert.assertEquals(payload,requestInfo.getBody());
      Assert.assertThat(requestInfo.getHeaders(),hasItems("Content-Type:application/json"));
    }
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
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload = "someValue";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload,0));
    writer.write(sinkRecords);
    Assert.assertEquals(1,restHelper.getCapturedRequests().size());
    for( RequestInfo requestInfo : restHelper.getCapturedRequests())
    {
      Assert.assertEquals(HttpSinkConfig.RequestMethod.DELETE.toString(),requestInfo.getMethod());
      Assert.assertEquals(endPoint,requestInfo.getUrl());
      Assert.assertEquals(payload,requestInfo.getBody());
      Assert.assertThat(requestInfo.getHeaders(),hasItems("Content-Type:application/json"));
    }
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
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload = "someValue";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload,0));
    writer.write(sinkRecords);
    Assert.assertEquals(1,restHelper.getCapturedRequests().size());
    for( RequestInfo requestInfo : restHelper.getCapturedRequests())
    {
      Assert.assertThat(requestInfo.getHeaders(),hasItems("Content-Type:application/json"));
      Assert.assertThat(requestInfo.getHeaders(),hasItems("Cache-Control:no-cache"));
    }
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
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload = "someValue";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload,0));
    writer.write(sinkRecords);
    Assert.assertEquals(1,restHelper.getCapturedRequests().size());
    for( RequestInfo requestInfo : restHelper.getCapturedRequests())
    {
      Assert.assertThat(requestInfo.getHeaders(),hasItems("Content-Type:application/json"));
      Assert.assertThat(requestInfo.getHeaders(),hasItems("Cache-Control:no-cache"));
    }
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
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload = "someValue";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload,0));
    writer.write(sinkRecords);
    Assert.assertEquals(1,restHelper.getCapturedRequests().size());
    for( RequestInfo requestInfo : restHelper.getCapturedRequests())
    {
      Assert.assertEquals(HttpSinkConfig.RequestMethod.POST.toString(),requestInfo.getMethod());
      Assert.assertEquals("/someTopic",requestInfo.getUrl());
      Assert.assertEquals(payload,requestInfo.getBody());
    }
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
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload = "someValue";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload,0));
    writer.write(sinkRecords);
    Assert.assertEquals(1,restHelper.getCapturedRequests().size());
    for( RequestInfo requestInfo : restHelper.getCapturedRequests())
    {
      Assert.assertEquals(HttpSinkConfig.RequestMethod.POST.toString(),requestInfo.getMethod());
      Assert.assertEquals("/someKey",requestInfo.getUrl());
      Assert.assertEquals(payload,requestInfo.getBody());
    }
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
    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue1";
    String payload2 = "someValue2";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload2,0));
    writer.write(sinkRecords);
    Assert.assertEquals(2,restHelper.getCapturedRequests().size());

    RequestInfo request1 = restHelper.getCapturedRequests().get(0);
    Assert.assertEquals(HttpSinkConfig.RequestMethod.POST.toString(),request1.getMethod());
    Assert.assertEquals("/test",request1.getUrl());
    Assert.assertEquals(payload1,request1.getBody());

    RequestInfo request2 = restHelper.getCapturedRequests().get(1);
    Assert.assertEquals(HttpSinkConfig.RequestMethod.POST.toString(),request2.getMethod());
    Assert.assertEquals("/test",request2.getUrl());
    Assert.assertEquals(payload2,request2.getBody());

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


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue1";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload1,0));
    writer.write(sinkRecords);
    Assert.assertEquals(1,restHelper.getCapturedRequests().size());

    RequestInfo request1 = restHelper.getCapturedRequests().get(0);
    Assert.assertEquals(HttpSinkConfig.RequestMethod.POST.toString(),request1.getMethod());
    Assert.assertEquals("/test",request1.getUrl());
    Assert.assertEquals("start" + payload1 + "end",request1.getBody());



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


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue1";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload1,0));
    writer.write(sinkRecords);
    Assert.assertEquals(1,restHelper.getCapturedRequests().size());

    RequestInfo request1 = restHelper.getCapturedRequests().get(0);
    Assert.assertEquals(HttpSinkConfig.RequestMethod.POST.toString(),request1.getMethod());
    Assert.assertEquals("/test",request1.getUrl());
    Assert.assertEquals("someKey" + payload1 + "someTopic",request1.getBody());



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


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue1";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload1,0));
    writer.write(sinkRecords);
    Assert.assertEquals(1,restHelper.getCapturedRequests().size());

    RequestInfo request1 = restHelper.getCapturedRequests().get(0);
    Assert.assertEquals(HttpSinkConfig.RequestMethod.POST.toString(),request1.getMethod());
    Assert.assertEquals("/test",request1.getUrl());
    Assert.assertTrue(request1.getBody().startsWith("batchPrefix"));
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


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue1";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload1,0));
    writer.write(sinkRecords);
    Assert.assertEquals(1,restHelper.getCapturedRequests().size());

    RequestInfo request1 = restHelper.getCapturedRequests().get(0);
    Assert.assertEquals(HttpSinkConfig.RequestMethod.POST.toString(),request1.getMethod());
    Assert.assertEquals("/test",request1.getUrl());
    Assert.assertTrue(request1.getBody().endsWith("batchSuffix"));
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


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue1";
    String payload2 = "someValue2";
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic",0,null,"someKey",null, payload2,0));
    writer.write(sinkRecords);
    Assert.assertEquals(1,restHelper.getCapturedRequests().size());

    RequestInfo request1 = restHelper.getCapturedRequests().get(0);
    Assert.assertEquals(HttpSinkConfig.RequestMethod.POST.toString(),request1.getMethod());
    Assert.assertEquals("/test",request1.getUrl());
    Assert.assertEquals("someKeysomeValue1someTopic,someKeysomeValue2someTopic",request1.getBody());
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


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue";
    sinkRecords.add(new SinkRecord("someTopic1",0,null,"someKey1",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic2",0,null,"someKey2",null, payload1,0));

    writer.write(sinkRecords);

    Assert.assertEquals(2,restHelper.getCapturedRequests().size());

    RequestInfo request1 = restHelper.getCapturedRequests().get(0);
    RequestInfo request2 = restHelper.getCapturedRequests().get(1);

    Assert.assertEquals("someKey1someValuesomeTopic1",request1.getBody());
    Assert.assertEquals("someKey2someValuesomeTopic2",request2.getBody());
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


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue";
    sinkRecords.add(new SinkRecord("someTopic1",0,null,"someKey1",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic2",0,null,"someKey2",null, payload1,0));

    writer.write(sinkRecords);

    Assert.assertEquals(1,restHelper.getCapturedRequests().size());

    RequestInfo request1 = restHelper.getCapturedRequests().get(0);

    Assert.assertEquals("someKey1someValuesomeTopic1,someKey2someValuesomeTopic2",request1.getBody());
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


    HttpSinkConfig config = new HttpSinkConfig(properties);

    HttpApiWriter writer = new HttpApiWriter(config);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String payload1 = "someValue";


    sinkRecords.add(new SinkRecord("someTopic1",0,null,"someKey1",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic2",0,null,"someKey2",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic1",0,null,"someKey2",null, payload1,0));
    sinkRecords.add(new SinkRecord("someTopic2",0,null,"someKey1",null, payload1,0));



    writer.write(sinkRecords);

    Assert.assertEquals(2,restHelper.getCapturedRequests().size());

    RequestInfo request1 = restHelper.getCapturedRequests().get(0);
    RequestInfo request2 = restHelper.getCapturedRequests().get(1);

    Assert.assertEquals("someKey1someValuesomeTopic1,someKey2someValuesomeTopic1",request1.getBody());
    Assert.assertEquals("someKey2someValuesomeTopic2,someKey1someValuesomeTopic2",request2.getBody());
  }


}