package stroom.stats;

import javax.ws.rs.core.Response;

import static org.assertj.core.api.Assertions.assertThat;

public class HttpAsserts {
    public static void assertAccepted(Response response){
        assertThat(response.getStatus()).isEqualTo(Response.Status.ACCEPTED.getStatusCode());
    }

    public static void assertOk(Response response){
        assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    public static void assertUnauthorized(Response response){
        assertThat(response.getStatus()).isEqualTo(Response.Status.UNAUTHORIZED.getStatusCode());
    }
}
