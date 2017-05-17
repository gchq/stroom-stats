package stroom.stats.service.resources;

import com.google.inject.Inject;

public class ApiResourceHealthCheck {

    private ApiResource apiResource;

    @Inject
    public ApiResourceHealthCheck(ApiResource apiResource){
        this.apiResource = apiResource;

    }
}
