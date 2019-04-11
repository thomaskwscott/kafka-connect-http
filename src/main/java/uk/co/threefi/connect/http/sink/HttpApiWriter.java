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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;


public class HttpApiWriter {

    private final HttpSinkConfig config;
    private static final Logger log = LoggerFactory.getLogger(HttpApiWriter.class);
    private Map<String, List<SinkRecord>> batches = new HashMap<>();
    private Map<String,Long> lastSentBatches = new HashMap<>();

    HttpApiWriter(final HttpSinkConfig config) {
        this.config = config;

    }

    public void write(final Collection<SinkRecord> records) throws IOException {

        for (SinkRecord record : records) {

            // build batch key
            String formattedKeyPattern = config.batchKeyPattern
                    .replace("${key}", record.key() == null ? "" : record.key().toString())
                    .replace("${topic}", record.topic());

            // add to batch and check for batch size limit
            if (!batches.containsKey(formattedKeyPattern)) {
                batches.put(formattedKeyPattern, new ArrayList<SinkRecord> (Arrays.asList(new SinkRecord[]{record})));
                // new batch needs to start linger timer
                lastSentBatches.put(formattedKeyPattern,System.currentTimeMillis());
            } else {
                batches.get(formattedKeyPattern).add(record);
            }
            if (batches.get(formattedKeyPattern).size() >= config.batchMaxSize) {
                sendBatch(formattedKeyPattern);
                lastSentBatches.put(formattedKeyPattern,System.currentTimeMillis());
            }

            flushBatches();
        }

    }

    public void flushBatches() throws IOException {
        // check linger
        for(Map.Entry<String,Long> entry: lastSentBatches.entrySet()) {
            if(entry.getValue() <= System.currentTimeMillis() - config.batchLingerMs && batches.get(entry.getKey()) !=null
                    &&  batches.get(entry.getKey()).size() > 0) {
                sendBatch(entry.getKey());
                lastSentBatches.put(entry.getKey(),System.currentTimeMillis());
            }
        }
    }

    private void sendBatch(String formattedKeyPattern) throws IOException {

        List<SinkRecord> records = batches.get(formattedKeyPattern);
        SinkRecord record0 = records.get(0);


        // build url - ${key} and ${topic} can be replaced with message values
        // the first record in the batch is used to build the url as we assume it will be consistent across all records.
        String formattedUrl = config.httpApiUrl
                .replace("${key}", record0.key() == null ? "" : record0.key().toString())
                .replace("${topic}", record0.topic());
        HttpSinkConfig.RequestMethod requestMethod = config.requestMethod;
        URL url = new URL(formattedUrl);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setDoOutput(true);
        con.setRequestMethod(requestMethod.toString());


        // add headers
        for (String headerKeyValue : config.headers.split(config.headerSeparator)) {
            if (headerKeyValue.contains(":")) {
                con.setRequestProperty(headerKeyValue.split(":")[0], headerKeyValue.split(":")[1]);
            }
        }

        StringBuilder builder = new StringBuilder(config.batchPrefix);
        int batchIndex=0;
        for(SinkRecord record : records) {
            String recordValue = buildRecord(record);
            builder.append(recordValue);
            batchIndex++;
            if (batchIndex < records.size()) {
                builder.append(config.batchSeparator);
            }
        }
        builder.append(config.batchSuffix);

        OutputStreamWriter writer = new OutputStreamWriter(con.getOutputStream(), "UTF-8");
        writer.write(builder.toString());
        writer.close();

        //clear batch
        batches.remove(formattedKeyPattern);
        lastSentBatches.remove(formattedKeyPattern);

        log.debug("Submitted payload: " + builder.toString()
                + ", url:" + formattedUrl);

        // get response
        int status = con.getResponseCode();
        if (status != 200) {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getErrorStream()));
            String inputLine;
            StringBuffer error = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                error.append(inputLine);
            }
            in.close();
            throw new IOException("HTTP Response code: " + status
                    + ", " + con.getResponseMessage() + ", " + error
                    + ", Submitted payload: " + builder.toString()
                    + ", url:" + formattedUrl);
        }
        log.debug(", response code: " + status
                + ", " + con.getResponseMessage()
                + ", headers: " + config.headers);

        // write the response to the log
        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer content = new StringBuffer();
        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }
        in.close();
        con.disconnect();
    }

    private String buildRecord(SinkRecord record) {
        // add payload
        String value = record.value().toString();

        // apply regexes
        int replacementIndex = 0;
        for (String pattern : config.regexPatterns.split(config.regexSeparator)) {
            String replacement = "";
            if (replacementIndex < config.regexReplacements.split(config.regexSeparator).length) {
                replacement = config.regexReplacements.split(config.regexSeparator)[replacementIndex]
                        .replace("${key}", record.key() == null ? "" : record.key().toString())
                        .replace("${topic}", record.topic());
            }
            value = value.replaceAll(pattern, replacement);
            replacementIndex++;
        }
        return value;
    }

}
