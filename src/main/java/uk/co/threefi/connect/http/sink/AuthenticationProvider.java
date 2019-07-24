package uk.co.threefi.connect.http.sink;

import java.io.IOException;

public interface AuthenticationProvider {
    String getBearerToken() throws IOException;
}
