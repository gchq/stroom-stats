package stroom.stats.service;

import com.google.common.base.Preconditions;
import stroom.stats.properties.StroomPropertyService;

/**
 * Defines the versioned services that stroom will present externally
 */
public enum RegisteredService {
    STROOM_STATS_V1(ExternalService.STROOM_STATS, ResourcePaths.STROOM_STATS, 1);

    private final ExternalService externalService;
    private final String subPath;
    private final int version;

    RegisteredService(final ExternalService externalService, final String subPath, final int version) {
        Preconditions.checkArgument(externalService.getType().equals(ExternalService.Type.SERVER) ||
        externalService.getType().equals(ExternalService.Type.CLIENT_AND_SERVER),
                "Incorrect type for defining as a registered service");
        this.externalService = externalService;
        this.subPath = subPath;
        this.version = version;
    }

    public String getVersionedPath() {
        return subPath + "/v" + version;
    }

    public String getSubPath() {
        return subPath;
    }

    public ExternalService getExternalService() {
        return externalService;
    }

    public String getVersionedServiceName(final StroomPropertyService stroomPropertyService) {
        return externalService.getVersionedServiceName(stroomPropertyService);
    }
}
