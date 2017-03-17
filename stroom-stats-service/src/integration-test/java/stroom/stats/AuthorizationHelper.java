/*
 * Copyright 2017 Crown Copyright
 *
 * This file is part of Stroom-Stats.
 *
 * Stroom-Stats is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Stroom-Stats is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Stroom-Stats.  If not, see <http://www.gnu.org/licenses/>.
 */

package stroom.stats;

import jersey.repackaged.com.google.common.base.Throwables;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.keys.HmacKey;
import org.jose4j.lang.JoseException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.jose4j.jws.AlgorithmIdentifiers.HMAC_SHA512;

public class AuthorizationHelper {
    // This token must match that in the applications config. I.e. config.yml:jwtTokenSecret
    private static final byte[] VALID_JWT_TOKEN_SECRET = "bd678197-a88e-499c-b03b-62c3dd7dfd2d".getBytes(UTF_8);
    private static final byte[] INVALID_JWT_TOKEN_SECRET = "bad-token".getBytes(UTF_8);

    public static String getHeaderWithValidCredentials()  {
        return "Bearer " + getToken(VALID_JWT_TOKEN_SECRET);
    }

    public static String getHeaderWithInvalidCredentials()  {
        return "Bearer " + getToken(INVALID_JWT_TOKEN_SECRET);
    }

    private static String getToken(byte[] jwtSecretToken) {
        return toToken(jwtSecretToken, getClaimsForUser("stroom-stats-service integration test"));
    }

    private static String toToken(byte[] key, JwtClaims claims) {
        final JsonWebSignature jws = new JsonWebSignature();
        jws.setPayload(claims.toJson());
        jws.setAlgorithmHeaderValue(HMAC_SHA512);
        jws.setKey(new HmacKey(key));
        jws.setDoKeyValidation(false);

        try {
            return jws.getCompactSerialization();
        }
        catch (JoseException e) { throw Throwables.propagate(e); }
    }

    private static JwtClaims getClaimsForUser(String user) {
        final JwtClaims claims = new JwtClaims();
        claims.setExpirationTimeMinutesInTheFuture(5);
        claims.setSubject(user);
        return claims;
    }
}
