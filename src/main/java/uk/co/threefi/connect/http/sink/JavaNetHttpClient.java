package uk.co.threefi.connect.http.sink;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaNetHttpClient {
    private static final Logger log = LoggerFactory.getLogger(JavaNetHttpClient.class);

    public Response makeRequest(final String requestMethod,
                                final String url,
                                final Map<String, String> headers,
                                final String payload) throws IOException {
        log.debug("{} {}", requestMethod, url);
        HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
        con.setDoOutput(true);
        con.setRequestMethod(requestMethod);
        headers.forEach(con::setRequestProperty);

        OutputStreamWriter writer = new OutputStreamWriter(con.getOutputStream(),
                StandardCharsets.UTF_8);
        writer.write(payload);
        writer.close();

        int status = con.getResponseCode();
        log.debug("{} {} => {} {}", requestMethod, url, status, con.getResponseMessage());
        if (status != 200) {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getErrorStream()));
            String inputLine;
            StringBuilder error = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                error.append(inputLine);
            }
            in.close();
            return new Response(status, con.getResponseMessage(), error.toString());
        }

        // write the response to the log
        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuilder content = new StringBuilder();
        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }
        in.close();
        con.disconnect();
        return new Response(status, con.getResponseMessage(), content.toString());
    }
}
