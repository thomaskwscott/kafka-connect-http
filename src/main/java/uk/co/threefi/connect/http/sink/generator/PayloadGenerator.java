package uk.co.threefi.connect.http.sink.generator;

import java.security.PrivateKey;
import java.time.Instant;
import java.util.Date;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

public class PayloadGenerator {
    private static final String URL_ENCODED_GRANT_TYPE =
            "urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer";
    private final PrivateKey privateKey;
    private final String issuer;
    private final String subject;
    private final String audience;

    public PayloadGenerator(PrivateKey privateKey, String issuer, String subject, String audience) {
        this.privateKey = privateKey;
        this.issuer = issuer;
        this.subject = subject;
        this.audience = audience;
    }

    public String generate(final Instant requestedExpiry) {
        return String.format("grant_type=%s&assertion=%s",
                             URL_ENCODED_GRANT_TYPE,
                             buildAssertion(requestedExpiry));
    }

    private String buildAssertion(final Instant requestedExpiry) {
        return Jwts.builder()
                .setExpiration(Date.from(requestedExpiry))
                .setIssuer(issuer)
                .setSubject(subject)
                .setAudience(audience)
                .signWith(privateKey, SignatureAlgorithm.RS256)
                .compact();
    }
}