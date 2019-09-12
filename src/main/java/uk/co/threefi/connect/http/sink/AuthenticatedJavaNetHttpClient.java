package uk.co.threefi.connect.http.sink;

import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthenticatedJavaNetHttpClient extends JavaNetHttpClient {
    private static final Logger log = LoggerFactory.getLogger(AuthenticatedJavaNetHttpClient.class);
    private AuthenticationProvider authenticationProvider;

    AuthenticatedJavaNetHttpClient(AuthenticationProvider authenticationProvider) {
        this.authenticationProvider = authenticationProvider;
    }

    @Override
    public Response makeRequest(final String requestMethod,
                                final String url,
                                final Map<String, String> headers,
                                final String payload) throws IOException {
        headers.put("Authorization", authenticationProvider.getBearerToken());
        Response response = super.makeRequest(requestMethod, url, headers, payload);
        if (response.getStatusCode() == 401) {
            log.debug("Request came back unauthorized; retrying with a fresh token");
            headers.put("Authorization", authenticationProvider.obtainNewBearerToken());
            return super.makeRequest(requestMethod, url, headers, payload);
        }
        return response;
    }
}