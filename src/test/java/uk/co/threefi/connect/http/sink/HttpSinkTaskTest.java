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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.easymock.EasyMockSupport;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HttpSinkTaskTest extends EasyMockSupport {

    @Test
    public void retries() throws IOException {
        final int maxRetries = 2;
        final int retryBackoffMs = 1000;

        Set<SinkRecord> records = Collections.singleton(new SinkRecord("stub", 0, null, null, null, "someVal", 0));
        final HttpApiWriter mockWriter = createMock(HttpApiWriter.class);
        SinkTaskContext ctx = createMock(SinkTaskContext.class);

        mockWriter.write(records);
        expectLastCall().andThrow(new IOException()).times(1 + maxRetries);

        ctx.timeout(retryBackoffMs);
        expectLastCall().times(maxRetries);

        HttpSinkTask task = new HttpSinkTask() {
            @Override
            protected void initWriter() {
                this.writer = mockWriter;
            }
        };
        task.initialize(ctx);

        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConfig.HTTP_API_URL, "stub");
        props.put(HttpSinkConfig.MAX_RETRIES, String.valueOf(maxRetries));
        props.put(HttpSinkConfig.RETRY_BACKOFF_MS, String.valueOf(retryBackoffMs));
        task.start(props);

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
            fail();
        } catch (RetriableException e) {
            fail("Non-retriable exception expected");
        } catch (ConnectException expected) {
            assertEquals(IOException.class, expected.getCause().getClass());
        }

        verifyAll();
    }

}
