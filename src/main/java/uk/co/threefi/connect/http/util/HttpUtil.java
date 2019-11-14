package uk.co.threefi.connect.http.util;

import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;

import uk.co.threefi.connect.http.sink.Response;

import java.util.Arrays;
import java.util.List;

public class HttpUtil {

    private static final List<Integer> SUCCESSFUL_STATUSES =
          Arrays.asList(HTTP_OK, HTTP_CREATED, HTTP_ACCEPTED, HTTP_NO_CONTENT);

    public static boolean isResponseSuccessful(final Response response) {
        return isResponseSuccessful(response.getStatusCode());
    }

    public static boolean isResponseSuccessful(final int status) {
        return SUCCESSFUL_STATUSES.contains(status);
    }
}
