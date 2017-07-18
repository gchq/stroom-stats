package stroom.stats.service;

import com.google.common.base.Preconditions;
import javaslang.Tuple2;
import org.apache.curator.x.discovery.ProviderStrategy;
import org.apache.curator.x.discovery.strategies.RandomStrategy;
import org.apache.curator.x.discovery.strategies.StickyStrategy;
import stroom.stats.properties.StroomPropertyService;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

public enum ExternalService {
    //TODO think index can be removed
//    INDEX ("stroomIndex"),
//    AUTHORISATION ("authorisation"),
//    AUTHENTICATION ("authentication");

    //stroom index involves multiple calls to fetch the data iteratively so must be sticky
    INDEX("stroomIndex", new StickyStrategy<>(new RandomStrategy<>())),
    //stateless so random strategy
    AUTHENTICATION("authentication", new RandomStrategy<>()),
    //stateless so random strategy
    AUTHORISATION("authorisation", new RandomStrategy<>());

    private static final String PROP_KEY_PREFIX = "stroom.services.";
    private static final String NAME_SUFFIX = ".name";
    private static final String VERSION_SUFFIX = ".version";
    private static final String DOC_REF_TYPE_SUFFIX = ".docRefType";

    //The serviceKey is a stroom specific abstraction of the service name, allowing the name to be set in properties
    //rather than hardcoded here.  The name that corresponds the serviceKey is what Curator registers services against.
    private final String serviceKey;
    private final ProviderStrategy<String> providerStrategy;

    /**
     * This maps doc ref types to services. I.e. if someone has the doc ref type they can get an ExternalService.
     */
    private static ConcurrentMap<String, ExternalService> docRefTypeToServiceMap = new ConcurrentHashMap<>();

    ExternalService(final String serviceKey, final ProviderStrategy<String> providerStrategy) {
        this.serviceKey = serviceKey;
        this.providerStrategy = providerStrategy;
    }

    public static Optional<ExternalService> getExternalService(final StroomPropertyService stroomPropertyService,
                                                               final String docRefType) {
        Preconditions.checkNotNull(docRefType);

        //lazy population of the map
        ExternalService requestedExternalService = docRefTypeToServiceMap.computeIfAbsent(docRefType, k ->
                Stream.of(ExternalService.values())
                        .map(externalService -> {
                            String type = stroomPropertyService.getProperty(
                                    PROP_KEY_PREFIX + externalService.getServiceKey() + DOC_REF_TYPE_SUFFIX)
                                    .orElse(null);
                            return new Tuple2<>(externalService, type);
                        })
                        .filter(tuple2 -> tuple2._2().equals(docRefType))
                        .map(Tuple2::_1)
                        .findFirst()
                        .orElse(null)
        );
        return Optional.ofNullable(requestedExternalService);
    }

    /**
     * This is the name of the service, as obtained from configuration.
     */
    public String getBaseServiceName(final StroomPropertyService stroomPropertyService) {
        String propKey = PROP_KEY_PREFIX + serviceKey + NAME_SUFFIX;
        return stroomPropertyService.getPropertyOrThrow(propKey);
    }

    public int getVersion(final StroomPropertyService stroomPropertyService) {
        String propKey = PROP_KEY_PREFIX + serviceKey + VERSION_SUFFIX;
        return stroomPropertyService.getIntProperty(propKey, 1);
    }

    public String getVersionedServiceName(final StroomPropertyService stroomPropertyService) {
        return getBaseServiceName(stroomPropertyService) + "-v" + getVersion(stroomPropertyService);
    }

    public ProviderStrategy<String> getProviderStrategy() {
        return providerStrategy;
    }

    /**
     * This is the value in the configuration, i.e. stroom.services.<serviceKey>.name.
     */
    public String getServiceKey() {
        return serviceKey;
    }
}