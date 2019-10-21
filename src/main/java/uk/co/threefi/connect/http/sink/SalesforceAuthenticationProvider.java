package uk.co.threefi.connect.http.sink;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SalesforceAuthenticationProvider implements AuthenticationProvider {
    private static final Logger log =
            LoggerFactory.getLogger(SalesforceAuthenticationProvider.class);
    private final String salesforceAuthRootUrl;
    private final JavaNetHttpClient httpClient;
    private final Clock clock;
    private final PayloadGenerator payloadGenerator;

    @VisibleForTesting
    BearerToken token;

    public SalesforceAuthenticationProvider(final String salesforceAuthRootUrl,
                                            final JavaNetHttpClient httpClient,
                                            final Clock clock,
                                            final PayloadGenerator payloadGenerator) {
        this.salesforceAuthRootUrl = salesforceAuthRootUrl;
        this.httpClient = httpClient;
        this.clock = clock;
        this.payloadGenerator = payloadGenerator;
    }

    @Override
    public String getBearerToken() throws IOException {
        if (!tokenIsValid()) {
            token = requestToken();
        }
        return String.format("Bearer %s", token.getToken());
    }

    @Override
    public String obtainNewBearerToken() throws IOException {
        token = null;
        return getBearerToken();
    }

    private boolean tokenIsValid() {
        return token != null && token.isValid(clock);
    }

    private BearerToken requestToken() throws IOException {
        Instant requestedExpiry = Instant.now(clock).plusSeconds(180);

        Response response = httpClient.makeRequest(
                "POST",
                String.format("%s/services/oauth2/token", salesforceAuthRootUrl),
                ImmutableMap.of("Content-Type", "application/x-www-form-urlencoded"),
                payloadGenerator.generate(requestedExpiry));
        if (response.getStatusCode() != 200) {
            throw new IOException(String.format("Request to %s/services/oauth2/token returned %s.",
                    salesforceAuthRootUrl, response));
        }
        log.info("Successful login to SF API.");
        log.debug("Login response: {}", response);
        JsonObject jsonBody = new JsonParser().parse(response.getBody()).getAsJsonObject();
        return new BearerToken(jsonBody.get("access_token").getAsString(),
                requestedExpiry.minusSeconds(5));
    }
}
