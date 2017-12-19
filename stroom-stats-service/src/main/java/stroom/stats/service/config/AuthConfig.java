package stroom.stats.service.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

public class AuthConfig {

    @NotNull
    @JsonProperty
    private String authorisationServiceUrl;

    @NotNull
    @JsonProperty
    private String authenticationServiceUrl;

    @NotNull
    @JsonProperty
    private String expectedIssuer;

    public String getAuthorisationServiceUrl() {
        return authorisationServiceUrl;
    }

    public String getAuthenticationServiceUrl() {
        return authenticationServiceUrl;
    }

    public String getExpectedIssuer() {
        return expectedIssuer;
    }
}
