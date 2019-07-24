package uk.co.threefi.connect.http.sink;

import io.jsonwebtoken.Jwts;
import java.security.PrivateKey;
import java.time.Instant;
import java.util.Date;

public class PayloadGenerator {
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

    public String generate(Instant requestedExpiry) {
        return Jwts.builder()
                .setExpiration(Date.from(requestedExpiry))
                .setIssuer(issuer)
                .setSubject(subject)
                .setAudience(audience)
                .signWith(privateKey)
                .compact();
    }
}
