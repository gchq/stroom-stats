package stroom.stats.service.auth;

import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.lang.JoseException;
import stroom.auth.service.ApiClient;
import stroom.auth.service.ApiException;
import stroom.auth.service.api.ApiKeyApi;
import stroom.auth.service.api.AuthenticationApi;
import stroom.stats.service.config.Config;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Optional;

import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;

@Singleton
public class JwtVerifier {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(JwtVerifier.class);

    private final ApiKeyApi apiKeyApi;
    private final AuthenticationApi authenticationApi;
    private final Config config;
    private final JwtConsumer jwtConsumer;
    private PublicJsonWebKey jwk;

    @Inject
    public JwtVerifier(Config config){
        this.config = config;

        ApiClient authServiceClient = new ApiClient();
        authServiceClient.setBasePath(config.getAuthConfig().getAuthenticationServiceUrl());
        authServiceClient.addDefaultHeader(AUTHORIZATION, "Bearer " + config.getAuthConfig().getApiKey());

        apiKeyApi = new ApiKeyApi(authServiceClient);
        authenticationApi = new AuthenticationApi(authServiceClient);
        jwtConsumer = newJwsConsumer();

        fetchNewPublicKeys();
    }

    public Optional<String> verify(String securityToken) {
        try {
            JwtClaims jwtClaims = jwtConsumer.processToClaims(securityToken);
            return Optional.of(jwtClaims.getSubject());
        } catch (InvalidJwtException | MalformedClaimException e) {
            return Optional.empty();
        }
    }

    public boolean hasBeenRevoked(String jwt) {
        try {
            authenticationApi.verifyToken(jwt);
            return false;
        } catch (ApiException e) {
            return true;
        }
    }

    private void fetchNewPublicKeys(){
        // We need to fetch the public key from the remote authentication service.
        try {
            String jwkAsJson = apiKeyApi.getPublicKey();

            jwk = RsaJsonWebKey.Factory.newPublicJwk(jwkAsJson);
        } catch (JoseException | ApiException e) {
            LOGGER.error("Unable to retrieve public key! Can't verify any API requests without the " +
                    "public key so all API requests must be refused until this the service is available again!");
        }
    }

    private JwtConsumer newJwsConsumer(){
        // If we don't have a JWK we can't create a consumer to verify anything.
        // Why might we not have one? If the remote authentication service was down when Stroom started
        // then we wouldn't. It might not be up now but we're going to try and fetch it.
        if(jwk == null){
            fetchNewPublicKeys();
        }

        JwtConsumerBuilder builder = new JwtConsumerBuilder()
                .setAllowedClockSkewInSeconds(30) // allow some leeway in validating time based claims to account for clock skew
                .setRequireSubject() // the JWT must have a subject claim
                .setVerificationKey(this.jwk.getPublicKey()) // verify the signature with the public key
                .setRelaxVerificationKeyValidation() // relaxes key length requirement
                .setJwsAlgorithmConstraints( // only allow the expected signature algorithm(s) in the given context
                        new AlgorithmConstraints(AlgorithmConstraints.ConstraintType.WHITELIST, // which is only RS256 here
                                AlgorithmIdentifiers.RSA_USING_SHA256))
                .setExpectedIssuer(this.config.getAuthConfig().getExpectedIssuer());
        return builder.build();
    }

}
