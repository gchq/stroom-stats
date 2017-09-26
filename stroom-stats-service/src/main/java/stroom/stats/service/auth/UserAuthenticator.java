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
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientResponse;
import stroom.stats.service.config.Config;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.util.Optional;

public class UserAuthenticator implements Authenticator<String, User> {

    private Config config;

    public UserAuthenticator(Config config) {
        this.config = config;
    }

    @Override
    public Optional<User> authenticate(String securityToken) throws AuthenticationException {
        Client client = ClientBuilder.newClient(new ClientConfig().register(ClientResponse.class));
        Response response = client
            .target(this.config.getAuthenticationServiceUrl() + "/verify/" + securityToken)
            .request()
            .get();

        if(response.getStatus() == Response.Status.UNAUTHORIZED.getStatusCode()){
            throw new AuthenticationException("Token verification failed: unauthorised.");
        }
        else if (response.getStatus() != Response.Status.OK.getStatusCode()){
            throw new AuthenticationException("Token verification failed: server response is " + response.getStatus());
        }

        String userEmail = response.readEntity(String.class);
        User user = new User(userEmail, securityToken);
        return Optional.of(user);
    }
}
