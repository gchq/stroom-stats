package stroom.stats;

import com.google.inject.Inject;

public class ApiResourceHealthCheck {

    private ApiResource apiResource;

    @Inject
    public ApiResourceHealthCheck(ApiResource apiResource){
        this.apiResource = apiResource;

    }
}
