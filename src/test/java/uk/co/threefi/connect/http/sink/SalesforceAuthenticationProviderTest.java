package uk.co.threefi.connect.http.sink;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.assertj.core.util.Strings;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static uk.co.threefi.connect.http.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class SalesforceAuthenticationProviderTest {
    private static final String EXPECTED_TOKEN = "00Dxx0000001gPL!AR8AQJXg5oj8jXSgxJfA0lBo" +
            "g.39AsX.LVpxezPwuX5VAIrrbbHMuol7GQxnMeYMN7cj8EoWr78nt1u44zU31IbYNNJguseu";
    private static final String EXPECTED_BEARER_TOKEN = String.format("Bearer %s", EXPECTED_TOKEN);
    private static final String VALID_TOKEN_RESPONSE =
            "{\"access_token\":\"00Dxx0000001gPL!AR8AQJXg5oj8jXSgxJfA0lBog." +
                    "39AsX.LVpxezPwuX5VAIrrbbHMuol7GQxnMeYMN7cj8EoWr78nt1u44zU31" +
                    "IbYNNJguseu\",\"scope\":\"web openid api id\",\"instance_url\":\"" +
                    "https://nutmeg.salesforce.com\",\"id\":\"" +
                    "https://nutmeg.salesforce.com" +
                    "/id/00Dxx0000001gPLEAY/005xx000001SwiUAAS\",\"token_type\":\"Bearer\"}";
    private static final String SALESFORCE_AUTH_ROOT_URL = "http://example.org/login";
    private static final String SALESFORCE_AUTH_FULL_URL =
            SALESFORCE_AUTH_ROOT_URL + "/services/oauth2/token";
    private static final String POST_METHOD = "POST";

    private static final Instant CLOCK_INSTANT =
            LocalDate.of(2019, 7, 24).atStartOfDay().toInstant(ZoneOffset.UTC);
    private static final String JWT_PAYLOAD = "Body";
    private static final ImmutableMap<String, String> AUTH_HEADERS = ImmutableMap.of("Content-Type", "application/x-www-form-urlencoded");

    @Mock
    private JavaNetHttpClient httpClient;

    @Mock
    private PayloadGenerator payloadGenerator;

    private SalesforceAuthenticationProvider provider;

    @Before
    public void setUp() {
        provider = new SalesforceAuthenticationProvider(SALESFORCE_AUTH_ROOT_URL, httpClient,
                Clock.fixed(CLOCK_INSTANT, ZoneOffset.UTC), payloadGenerator);
        when(payloadGenerator.generate(CLOCK_INSTANT.plusSeconds(180))).thenReturn(JWT_PAYLOAD);
    }

    @Test
    public void whenNoTokenIsCurrentlyStoredRequestOneAndCacheIt() throws Exception {
        when(httpClient.makeRequest(POST_METHOD, SALESFORCE_AUTH_FULL_URL, AUTH_HEADERS, JWT_PAYLOAD))
                .thenReturn(new Response(200, "OK", VALID_TOKEN_RESPONSE));

        assertThat(provider.getBearerToken()).isEqualTo(EXPECTED_BEARER_TOKEN);
        verify(httpClient, times(1))
                .makeRequest(POST_METHOD, SALESFORCE_AUTH_FULL_URL, AUTH_HEADERS, JWT_PAYLOAD);
        assertThat(provider.token)
                .hasToken(EXPECTED_TOKEN)
                .hasExpiry(CLOCK_INSTANT.plusSeconds(175));
    }

    @Test
    public void whenTokenHasExpiredRequestANewOneAndCacheIt() throws Exception {
        provider.token = new BearerToken(EXPECTED_TOKEN, CLOCK_INSTANT.minusSeconds(300));

        when(httpClient.makeRequest(POST_METHOD, SALESFORCE_AUTH_FULL_URL, AUTH_HEADERS, JWT_PAYLOAD))
                .thenReturn(new Response(200, "OK", VALID_TOKEN_RESPONSE));

        assertThat(provider.getBearerToken()).isEqualTo(EXPECTED_BEARER_TOKEN);
        verify(httpClient, times(1))
                .makeRequest(POST_METHOD, SALESFORCE_AUTH_FULL_URL, AUTH_HEADERS, JWT_PAYLOAD);
        assertThat(provider.token)
                .hasToken(EXPECTED_TOKEN)
                .hasExpiry(CLOCK_INSTANT.plusSeconds(175));
    }

    @Test
    public void whenTokenRetrievalFailsPassOnTheException() throws Exception {
        when(httpClient.makeRequest(POST_METHOD, SALESFORCE_AUTH_FULL_URL, AUTH_HEADERS, JWT_PAYLOAD))
                .thenThrow(IOException.class);

        assertThatExceptionOfType(IOException.class).isThrownBy(() -> provider.getBearerToken());
        verify(httpClient, times(1))
                .makeRequest(POST_METHOD, SALESFORCE_AUTH_FULL_URL, AUTH_HEADERS, JWT_PAYLOAD);
        assertThat(provider.token).isNull();
    }

    @Test
    public void whenTokenIsStoredAndValidReturnIt() throws Exception {
        provider.token = new BearerToken(EXPECTED_TOKEN, CLOCK_INSTANT.plusSeconds(175));

        assertThat(provider.getBearerToken()).isEqualTo(EXPECTED_BEARER_TOKEN);
        verifyZeroInteractions(httpClient);
        assertThat(provider.token)
                .hasToken(EXPECTED_TOKEN)
                .hasExpiry(CLOCK_INSTANT.plusSeconds(175));
    }
}