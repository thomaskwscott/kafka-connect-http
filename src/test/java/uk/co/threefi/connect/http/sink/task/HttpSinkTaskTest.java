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

package uk.co.threefi.connect.http.sink.task;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.fail;
import static uk.co.threefi.connect.http.sink.config.HttpSinkConfig.RESPONSE_PRODUCER;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.zookeeper.proto.ErrorResponse;
import org.easymock.EasyMockSupport;
import org.junit.Test;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import junit.framework.AssertionFailedError;
import uk.co.threefi.connect.http.sink.config.HttpSinkConfig;
import uk.co.threefi.connect.http.sink.dto.ResponseError;
import uk.co.threefi.connect.http.sink.handler.ResponseHandler;
import uk.co.threefi.connect.http.sink.writer.HttpApiWriter;

public class HttpSinkTaskTest extends EasyMockSupport {

    @Test
    public void canProcessWhenNoErrorsFound()
          throws IOException, ExecutionException, InterruptedException, TimeoutException {
        Set<SinkRecord> records = Collections
              .singleton(new SinkRecord("stub", 0, null, null, null, "someVal", 0));
        final HttpApiWriter mockWriter = createMock(HttpApiWriter.class);

        mockWriter.write(records);
        expectLastCall().andReturn(new HashSet<ErrorResponse>()).times(1);

        HttpSinkTask task = new HttpSinkTask() {
            @Override
            protected void init() {
                this.writer = mockWriter;
            }
        };

        Map<String, String> properties = getProperties(1);
        task.start(properties);
        replayAll();
        try {
            task.put(records);
        } catch (RetriableException e) {
            fail("No exception expected");
        }
        verifyAll();
    }

    @Test
    public void canIgnoreWhenRecordsAreEmpty()
          throws InterruptedException, ExecutionException, TimeoutException, IOException {
        Set<SinkRecord> records = new HashSet<>();
        final HttpApiWriter mockWriter = createMock(HttpApiWriter.class);

        mockWriter.write(records);
        expectLastCall().andThrow(new AssertionFailedError("Write method should not be called"))
              .anyTimes();
        HttpSinkTask task = new HttpSinkTask() {
            @Override
            protected void init() {
                this.writer = mockWriter;
            }
        };
        Map<String, String> properties = getProperties(0);
        task.start(properties);
        task.put(records);
    }

    @Test(expected = ConnectException.class)
    public void canThrowConnectionErrorOnInitError() {
        HttpSinkTask task = new HttpSinkTask() {
            @Override
            protected void init() throws Exception {
                throw new Exception("Exception");
            }
        };
        Map<String, String> properties = getProperties(2);
        task.start(properties);
    }

    @Test
    public void canRetryAndHandleError()
          throws IOException, ExecutionException, InterruptedException, TimeoutException {
        final int maxRetries = 2;
        final int retryBackoffMs = 1000;

        Set<SinkRecord> records = Collections
              .singleton(new SinkRecord("stub", 0, null, null, null, "someVal", 0));
        final HttpApiWriter mockWriter = createMock(HttpApiWriter.class);
        ResponseHandler responseHandlerMock = createMock(ResponseHandler.class);
        SinkTaskContext ctx = createMock(SinkTaskContext.class);

        Set<ResponseError> errorResponses =
              Stream.of(new ResponseError("Key", "ErrorMessage")).collect(
                    Collectors.toSet());
        mockWriter.write(records);
        expectLastCall().andReturn(errorResponses).times(maxRetries + 1);

        ctx.timeout(retryBackoffMs);
        expectLastCall().times(maxRetries);

        responseHandlerMock.handleErrors(anyObject(),anyObject());
        expectLastCall().times(1);

        HttpSinkTask task = new HttpSinkTask() {
            @Override
            protected void init() {
                this.writer = mockWriter;
                this.responseHandler = responseHandlerMock;
            }
        };
        task.initialize(ctx);

        Map<String, String> properties = getProperties(maxRetries);
        task.start(properties);

        replayAll();

        try {
            task.put(records);
            fail();
        } catch (RetriableException expected) {
        }
        try {
            task.put(records);
            fail();
        } catch (RetriableException expected) {
        }
        try {
            task.put(records);
        } catch (Exception e) {
            fail("No exception is expected in the last retry");
        }
        verifyAll();
    }

    private Map<String, String> getProperties(int maxRetries) {
        Map<String, String> properties = new HashMap<>();
        properties.put(HttpSinkConfig.HTTP_API_URL, "stub");
        properties.put(HttpSinkConfig.MAX_RETRIES, String.valueOf(maxRetries));
        properties.put(HttpSinkConfig.RETRY_BACKOFF_MS, String.valueOf(1000));
        properties.put(HttpSinkConfig.KEY_CONVERTER, StringConverter.class.getName());
        properties.put(HttpSinkConfig.VALUE_CONVERTER, AvroConverter.class.getName());
        properties.put(HttpSinkConfig.VALUE_CONVERTER_SR_URL, "http://localhost:8081");

        properties.put(RESPONSE_PRODUCER + ProducerConfig.RETRIES_CONFIG, "1");
        properties.put(RESPONSE_PRODUCER + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
              "http://localhost:9092");
        properties.put(RESPONSE_PRODUCER + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              StringSerializer.class.getName());
        properties
              .put(RESPONSE_PRODUCER + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    KafkaAvroSerializer.class.getName());
        properties.put(RESPONSE_PRODUCER + "schema.registry.url", "http://localhost:8081");
        return properties;
    }

}
