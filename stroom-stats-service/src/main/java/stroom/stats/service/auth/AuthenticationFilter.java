package stroom.stats.service.auth;

import com.github.toastshaman.dropwizard.auth.jwt.JwtAuthFilter;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.keys.HmacKey;
import stroom.stats.service.config.Config;

public class AuthenticationFilter {

    public static JwtAuthFilter<User> get(Config config)  {
        final JwtConsumer consumer = new JwtConsumerBuilder()
                .setAllowedClockSkewInSeconds(30) // allow some leeway in validating time based claims to account for clock skew
                .setRequireExpirationTime() // the JWT must have an expiration time
                .setRequireSubject() // the JWT must have a subject claim
                .setVerificationKey(new HmacKey(config.getJwtTokenSecret())) // verify the signature with the public key
                .setRelaxVerificationKeyValidation() // relaxes key length requirement
                .setExpectedIssuer("stroom")
                .build();

        return new JwtAuthFilter.Builder<User>()
                .setJwtConsumer(consumer)
                .setRealm("realm")
                .setPrefix("Bearer")
                .setAuthenticator(new UserAuthenticator())
                .buildAuthFilter();
    }
}
