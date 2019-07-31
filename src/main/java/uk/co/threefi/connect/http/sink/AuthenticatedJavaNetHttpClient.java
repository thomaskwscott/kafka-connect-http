package uk.co.threefi.connect.http.sink;

import java.io.IOException;
import java.util.Map;

public class AuthenticatedJavaNetHttpClient extends JavaNetHttpClient {
    private AuthenticationProvider authenticationProvider;

    public AuthenticatedJavaNetHttpClient(AuthenticationProvider authenticationProvider) {
        this.authenticationProvider = authenticationProvider;
    }

    @Override
    public Response makeRequest(final String requestMethod,
                                final String url,
                                final Map<String, String> headers,
                                final String payload) throws IOException {
        headers.put("Authorization", authenticationProvider.getBearerToken());
        return super.makeRequest(requestMethod, url, headers, payload);
    }
}
