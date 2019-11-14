package uk.co.threefi.connect.http.sink.dto;

import java.util.Objects;
import java.util.StringJoiner;

public class Response {
    private final int statusCode;
    private final String statusMessage;
    private final String body;

    public Response(int statusCode, String statusMessage, String body) {
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.body = body;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public String getBody() {
        return body;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Response response = (Response) o;
        return statusCode == response.statusCode &&
                Objects.equals(statusMessage, response.statusMessage) &&
                Objects.equals(body, response.body);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statusCode, statusMessage, body);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Response.class.getSimpleName() + "[", "]")
                .add("statusCode=" + statusCode)
                .add("statusMessage='" + statusMessage + "'")
                .add("body='" + body + "'")
                .toString();
    }
}
