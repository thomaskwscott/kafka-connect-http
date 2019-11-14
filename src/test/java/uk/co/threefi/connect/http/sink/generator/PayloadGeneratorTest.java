package uk.co.threefi.connect.http.sink.generator;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.co.threefi.connect.http.Assertions.assertThat;

import java.security.KeyPair;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;

public class PayloadGeneratorTest {
    private static final Instant NOW =
            LocalDate.now().plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC);
    private static final String AUDIENCE = "https://test.salesforce.com";
    private static final String ISSUER = "Issuer";
    private static final String SUBJECT = "Subject";
    private static final Pattern TOKEN_REQUEST_PATTERN =
            Pattern.compile("^grant_type=(.*)&assertion=(.*)$");

    private static KeyPair pair;

    private PayloadGenerator payloadGenerator;

    @BeforeClass
    public static void generateKeyPair() {
        pair = Keys.keyPairFor(SignatureAlgorithm.RS512 );
    }

    @Before
    public void setUp() {
        payloadGenerator = new PayloadGenerator(pair.getPrivate(), ISSUER, SUBJECT, AUDIENCE);
    }

    @Test
    public void generatesPayloadUsingSuppliedKeysAndCredentials() {
        String payload = payloadGenerator.generate(NOW);

        Matcher matcher = TOKEN_REQUEST_PATTERN.matcher(payload);
        assertThat(matcher.matches()).isTrue();

        assertThat(matcher.group(1))
                .isEqualTo("urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer");

        Jws<Claims> jws = Jwts.parser()
                .setSigningKey(pair.getPublic())
                .parseClaimsJws(matcher.group(2));

        assertThat(jws.getHeader().getAlgorithm()).isEqualTo(SignatureAlgorithm.RS256.getValue());

        Claims claims = jws
                .getBody();
        assertThat(claims)
                .hasExpiration(Date.from(NOW))
                .hasIssuer(ISSUER)
                .hasSubject(SUBJECT)
                .hasAudience(AUDIENCE);
    }
}