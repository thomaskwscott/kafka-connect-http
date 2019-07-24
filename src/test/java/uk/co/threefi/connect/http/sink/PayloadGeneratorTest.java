package uk.co.threefi.connect.http.sink;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.security.KeyPair;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static uk.co.threefi.connect.http.Assertions.assertThat;

public class PayloadGeneratorTest {
    private static final Instant NOW =
            LocalDate.now().plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC);
    private static final String AUDIENCE = "https://test.salesforce.com";
    private static final String ISSUER = "Issuer";
    private static final String SUBJECT = "Subject";

    private static KeyPair pair;

    private PayloadGenerator payloadGenerator;

    @BeforeClass
    public static void generateKeyPair() {
        pair = Keys.keyPairFor(SignatureAlgorithm.RS256);
    }

    @Before
    public void setUp() {
        payloadGenerator = new PayloadGenerator(pair, ISSUER, SUBJECT, AUDIENCE);
    }

    @Test
    public void generatesPayloadUsingSuppliedKeysAndCredentials() {
        String payload = payloadGenerator.generate(NOW);

        Claims claims = Jwts.parser()
                .setSigningKey(pair.getPublic())
                .parseClaimsJws(payload)
                .getBody();
        assertThat(claims)
                .hasExpiration(Date.from(NOW))
                .hasIssuer(ISSUER)
                .hasSubject(SUBJECT)
                .hasAudience(AUDIENCE);
    }
}