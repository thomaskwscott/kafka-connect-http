package uk.co.threefi.connect.http.sink;

import io.jsonwebtoken.Jwts;
import java.security.KeyPair;
import java.time.Instant;
import java.util.Date;

public class PayloadGenerator {
    private final KeyPair keyPair;
    private final String issuer;
    private final String subject;
    private final String audience;

    public PayloadGenerator(KeyPair keyPair, String issuer, String subject, String audience) {
        this.keyPair = keyPair;
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
                .signWith(keyPair.getPrivate())
                .compact();
    }
}
