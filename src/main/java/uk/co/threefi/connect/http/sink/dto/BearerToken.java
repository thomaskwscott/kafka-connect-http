package uk.co.threefi.connect.http.sink.dto;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.StringJoiner;

public class BearerToken {
    private final String token;
    private final Instant expiry;

    public BearerToken(String token, Instant expiry) {
        this.token = token;
        this.expiry = expiry;
    }

    public String getToken() {
        return token;
    }

    public Instant getExpiry() {
        return expiry;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BearerToken that = (BearerToken) o;
        return Objects.equals(token, that.token);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", BearerToken.class.getSimpleName() + "[", "]")
                .add("token='" + token + "'")
                .add("expiry=" + expiry)
                .toString();
    }

    public boolean isValid(final Clock clock) {
        return Instant.now(clock).isBefore(expiry);
    }
}
