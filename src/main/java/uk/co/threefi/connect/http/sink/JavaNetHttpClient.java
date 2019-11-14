package uk.co.threefi.connect.http.sink;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.co.threefi.connect.http.util.HttpUtil;

public class JavaNetHttpClient {
    private static final Logger log = LoggerFactory.getLogger(JavaNetHttpClient.class);

    public Response makeRequest(final String requestMethod,
                                final String url,
                                final Map<String, String> headers,
                                final String payload) throws IOException {
        log.debug("{} {}", requestMethod, url);
        log.debug("Headers {}", headers);
        log.debug("Payload {}", payload);

        final HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
        con.setDoOutput(true);
        con.setRequestMethod(requestMethod);
        headers.forEach(con::setRequestProperty);

        final OutputStreamWriter writer = new OutputStreamWriter(con.getOutputStream(),
                                                                 StandardCharsets.UTF_8);
        writer.write(payload);
        writer.close();

        final int status = con.getResponseCode();
        log.info("Got response with: " +
                "{} {} => {} {}", requestMethod, url, status, con.getResponseMessage());

        if (!HttpUtil.isResponseSuccessful(status)) {
            return obtainResponse(status, con.getErrorStream(), con);
        }

        // write the response to the log
        return obtainResponse(status, con.getInputStream(), con);
    }

    private Response obtainResponse(final int status,
                                    final InputStream inputStream,
                                    final HttpURLConnection con) throws IOException {
        final BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
        final StringBuilder content = new StringBuilder();

        String inputLine;

        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }

        in.close();
        con.disconnect();

        return new Response(status, con.getResponseMessage(), content.toString());
    }
}
