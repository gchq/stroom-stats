package stroom.stats.service.auth;

import stroom.stats.service.config.Config;

public class AuthenticationFilter {
    public static JwtVerificationFilter<User> get(Config config)  {
        return new JwtVerificationFilter.Builder<User>()
            .setConfig(config)
            .setRealm("realm")
            .setPrefix("Bearer")
            .setAuthenticator(new UserAuthenticator(config))
            .buildAuthFilter();
    }
}
