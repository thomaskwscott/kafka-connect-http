package uk.co.threefi.connect.http.sink;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static uk.co.threefi.connect.http.sink.RequestInfoAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AuthenticatedJavaNetHttpClientTest {
    private static final String PAYLOAD = "Test Payload";
    private static final String POST = "POST";
    private final RestHelper restHelper = new RestHelper();
    private String url;

    @Mock
    private AuthenticationProvider authenticationProvider;

    @InjectMocks
    private AuthenticatedJavaNetHttpClient client;

    @Before
    public void setUp() throws Exception {
        restHelper.start();
        url = String.format("http://localhost:%s/test", restHelper.getPort());
    }

    @After
    public void tearDown() throws Exception {
        restHelper.stop();
        restHelper.flushCapturedRequests();
    }

    @Test
    public void appendsAnAuthenticationHeaderToTheRequest() throws Exception {
        when(authenticationProvider.getBearerToken()).thenReturn("Bearer aaaa.bbbb.cccc");

        client = new AuthenticatedJavaNetHttpClient(authenticationProvider);
        client.makeRequest(
                POST, url,
                Maps.newHashMap(ImmutableMap.of(
                        "Accept", "application/json",
                        "Content-Type", "application/json"
                )), PAYLOAD);
        assertThat(restHelper.getCapturedRequests(), RequestInfoAssert.class)
                .hasSize(1)
                .first()
                .hasMethod(POST)
                .hasUrl("/test")
                .hasHeaders(
                        "Accept:application/json",
                        "Content-Type:application/json",
                        "Authorization:Bearer aaaa.bbbb.cccc"
                )
                .hasBody(PAYLOAD);
    }

    @Test
    public void whenAuthenticationIsNotPossibleAnExceptionIsThrown() throws Exception {
        when(authenticationProvider.getBearerToken()).thenThrow(IOException.class);

        client = new AuthenticatedJavaNetHttpClient(authenticationProvider);
        assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> client.makeRequest(
                        POST, url,
                        Maps.newHashMap(ImmutableMap.of(
                                "Accept", "application/json",
                                "Content-Type", "application/json"
                        )), PAYLOAD));
    }

    @Test
    public void whenAnAuthenticationErrorOccursReacquireTheBearerTokenAndRetryOnce() throws Exception {
        when(authenticationProvider.getBearerToken()).thenReturn("Bearer aaaa.bbbb.cccc");
        when(authenticationProvider.obtainNewBearerToken()).thenReturn("Bearer dddd.eeee.ffff");

        client = new AuthenticatedJavaNetHttpClient(authenticationProvider);
        client.makeRequest(
                POST, String.format("http://localhost:%s/unauthorized", restHelper.getPort()),
                Maps.newHashMap(ImmutableMap.of(
                        "Accept", "application/json",
                        "Content-Type", "application/json"
                )), PAYLOAD);
        assertThat(restHelper.getCapturedRequests()).hasSize(2);
        assertThat(restHelper.getCapturedRequests().get(0))
                .hasMethod(POST)
                .hasUrl("/unauthorized")
                .hasHeaders(
                        "Accept:application/json",
                        "Content-Type:application/json",
                        "Authorization:Bearer aaaa.bbbb.cccc"
                )
                .hasBody(PAYLOAD);
        assertThat(restHelper.getCapturedRequests().get(1))
                .hasMethod(POST)
                .hasUrl("/unauthorized")
                .hasHeaders(
                        "Accept:application/json",
                        "Content-Type:application/json",
                        "Authorization:Bearer dddd.eeee.ffff"
                )
                .hasBody(PAYLOAD);
    }
}