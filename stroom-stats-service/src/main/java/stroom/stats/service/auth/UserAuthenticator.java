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

package stroom.stats.service.auth;

import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Optional;

@Singleton
public class UserAuthenticator implements Authenticator<String, User> {
    public static final Logger LOGGER = LoggerFactory.getLogger(UserAuthenticator.class);

    private JwtVerifier jwtVerifier;

    @Inject
    public UserAuthenticator(JwtVerifier jwtVerifier) {
        this.jwtVerifier = jwtVerifier;
    }

    @Override
    public Optional<User> authenticate(String securityToken) throws AuthenticationException {
        Optional<String> userEmail = jwtVerifier.verify(securityToken);
        boolean hasBeenRevoked = jwtVerifier.hasBeenRevoked(securityToken);

        if(userEmail.isPresent() && !hasBeenRevoked){
            return Optional.of(new User(userEmail.get(), securityToken));
        }

        return Optional.empty();
    }


}
