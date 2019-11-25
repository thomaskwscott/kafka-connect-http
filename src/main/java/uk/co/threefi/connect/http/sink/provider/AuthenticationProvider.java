package uk.co.threefi.connect.http.sink.provider;

import java.io.IOException;

public interface AuthenticationProvider {
    String getBearerToken() throws IOException;

    String obtainNewBearerToken() throws IOException;
}
