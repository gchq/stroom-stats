package stroom.stats.service.auth;

import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.service.config.Config;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.security.Principal;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;

@Priority(Priorities.AUTHENTICATION)
public class JwtVerificationFilter<P extends Principal> extends AuthFilter<String, P> {
  private static final Logger LOGGER = LoggerFactory.getLogger(JwtVerificationFilter.class);

  private Config config;
  private final String prefix = "Bearer";

  public JwtVerificationFilter(Config config){
    this.config = config;
  }

  /**
   * Gets the token from the request and verifies it with the authentication service.
   *
   * If there's no token, of if verification fails, then this throws an exception to indicate the request has
   * failed authentication.
   */
  @Override
  public void filter(final ContainerRequestContext requestContext) throws IOException {
    final Optional<String> optionalToken = getTokenFromHeader(requestContext.getHeaders());

    if (!optionalToken.isPresent()) {
      throw new WebApplicationException(unauthorizedHandler.buildResponse(prefix, realm));
    }

    final Optional<P> optionalUser;
    try {
      optionalUser = authenticator.authenticate(optionalToken.get());
    } catch (AuthenticationException e) {
      LOGGER.info("Authentication failed");
      throw new WebApplicationException(unauthorizedHandler.buildResponse(prefix, realm));
    }

    // We need to set up the security context so that our endpoints have a User to work with.
    requestContext.setSecurityContext(new SecurityContext() {
      @Override
      public Principal getUserPrincipal() {
        return optionalUser.get();
      }

      @Override
      public boolean isUserInRole(String role) {
        return true;
      }

      @Override
      public boolean isSecure() {
        return requestContext.getSecurityContext().isSecure();
      }

      @Override
      public String getAuthenticationScheme() {
        return "Bearer";
      }
    });
  }

  private Optional<String> getTokenFromHeader(MultivaluedMap<String, String> headers) {
    final String authorisationHeader = headers.getFirst(AUTHORIZATION);
    if (authorisationHeader != null) {
      int delimiterLocation = authorisationHeader.indexOf(' ');
      if (delimiterLocation > 0) {
        final String authenticationScheme = authorisationHeader.substring(0, delimiterLocation);
        if (prefix.equalsIgnoreCase(authenticationScheme)) {
          final String token = authorisationHeader.substring(delimiterLocation + 1);
          return Optional.of(token);
        }
      }
    }
    return Optional.empty();
  }

  public static class Builder<P extends Principal> extends AuthFilterBuilder<String, P, JwtVerificationFilter<P>> {
    private Config config;

    public Builder<P> setConfig(Config config) {
      this.config = config;
      return this;
    }

    @Override
    protected JwtVerificationFilter<P> newInstance() {
      checkNotNull(config, "Config is not set");
      return new JwtVerificationFilter<>(config);
    }
  }

}
