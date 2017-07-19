package stroom.stats.service;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Preconditions;
import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.curator.x.discovery.UriSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.mixins.HasHealthCheck;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.service.config.Config;

import javax.inject.Inject;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Responsible for registering stroom's various externally exposed services with service discovery
 */
public class ServiceDiscoveryRegistrar implements HasHealthCheck {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryRegistrar.class);

    private static final String PROP_KEY_SERVICE_HOST_OR_IP = "stroom.stats.serviceDiscovery.servicesHostNameOrIpAddress";
    private static final String PROP_KEY_SERVICE_PORT = "stroom.stats.serviceDiscovery.servicesPort";

    private HealthCheck.Result health;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final StroomPropertyService stroomPropertyService;
    private final String hostNameOrIpAddress;
    private final int servicePort;

    @Inject
    public ServiceDiscoveryRegistrar(final Config config,
                                     final ServiceDiscoveryManager serviceDiscoveryManager,
                                     final StroomPropertyService stroomPropertyService) {

        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.stroomPropertyService = stroomPropertyService;
        this.hostNameOrIpAddress = getHostOrIp(stroomPropertyService);

        this.servicePort = stroomPropertyService.getIntProperty(PROP_KEY_SERVICE_PORT)
                .orElseGet(() -> getPort(config));

        health = HealthCheck.Result.unhealthy("Not yet initialised...");
        this.serviceDiscoveryManager.registerStartupListener(this::curatorStartupListener);
    }

    private String getHostOrIp(final StroomPropertyService stroomPropertyService) {
        String hostOrIp = stroomPropertyService.getProperty(PROP_KEY_SERVICE_HOST_OR_IP)
                .orElseGet(this::getLocalHostnameOrAddress);
        return hostOrIp;
    }

    private String getLocalHostnameOrAddress() {
        String hostOrIp = null;
        try {
            hostOrIp = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            LOGGER.warn("Unable to determine hostname of this instance due to error. Will try to get IP address instead", e);
        }

        if (hostOrIp == null || hostOrIp.isEmpty()) {
            try {
                hostOrIp = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                throw new RuntimeException(String.format("Error establishing hostname or IP address of this instance"), e);
            }
        }
        return hostOrIp;
    }

    private void curatorStartupListener(ServiceDiscovery<String> serviceDiscovery) {
        try {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("Successfully registered the following services: ");

            Map<String, String> services = new TreeMap<>();
            Arrays.stream(RegisteredService.values())
                    .forEach(registeredService -> {
                        ServiceInstance<String> serviceInstance = registerResource(
                                registeredService,
                                serviceDiscovery);
                        services.put(registeredService.getVersionedServiceName(stroomPropertyService), serviceInstance.buildUriSpec());
                    });

            health = HealthCheck.Result.builder()
                    .healthy()
                    .withMessage("Services registered")
                    .withDetail("registered-services", services)
                    .build();

            LOGGER.info("All service instances created successfully.");
        } catch (Exception e) {
            health = HealthCheck.Result.unhealthy("Service instance creation failed!", e);
            LOGGER.error("Service instance creation failed!", e);
            throw new RuntimeException("Service instance creation failed!", e);
        }
    }

    private ServiceInstance<String> registerResource(final RegisteredService registeredService,
                                                     final ServiceDiscovery<String> serviceDiscovery) {
        try {
            UriSpec uriSpec = new UriSpec("{scheme}://{address}:{port}" +
                    ResourcePaths.ROOT_PATH +
                    registeredService.getVersionedPath());

            ServiceInstance<String> serviceInstance = ServiceInstance.<String>builder()
                    .serviceType(ServiceType.DYNAMIC) //==ephemeral zk nodes so instance will disappear if we lose zk conn
                    .uriSpec(uriSpec)
                    .name(registeredService.getVersionedServiceName(stroomPropertyService))
                    .address(hostNameOrIpAddress)
                    .port(servicePort)
                    .build();

            LOGGER.info("Attempting to register '{}' with service discovery at {}",
                    registeredService.getVersionedServiceName(stroomPropertyService), serviceInstance.buildUriSpec());

            Preconditions.checkNotNull(serviceDiscovery).registerService(serviceInstance);

            LOGGER.info("Successfully registered '{}' service.", registeredService.getVersionedServiceName(stroomPropertyService));
            return serviceInstance;
        } catch (Exception e) {
            throw new RuntimeException("Failed to register service " + registeredService.getVersionedServiceName(stroomPropertyService), e);
        }
    }

    //TODO registering needs to go somewhere else, see gh-19
    private static ServiceInstance<String> getThisServiceInstance(Config config) throws Exception {
        String ipAddress = InetAddress.getLocalHost().getHostAddress();
        int port = getPort(config);

        ServiceInstance<String> thisInstance = ServiceInstance.<String>builder()
                .name("stroom-stats-v1") //TODO should be in config
                .address(ipAddress) //TODO should be overridable in config
                .port(port) //TODO should be overridable in config
                .build();
        return thisInstance;
    }

    private static int getPort(Config config) {
        int port = 0;
        DefaultServerFactory serverFactory = (DefaultServerFactory) config.getServerFactory();
        List<ConnectorFactory> connectorFactories = serverFactory.getApplicationConnectors();
        if (Preconditions.checkNotNull(connectorFactories).size() != 1) {
            throw new RuntimeException(
                    String.format("Unexpected number of connectorFactories %s, check 'applicationConnectors' in the YAML config",
                            connectorFactories.size()));
        }

        HttpConnectorFactory connector = (HttpConnectorFactory) connectorFactories.get(0);
        if (connector.getClass().isAssignableFrom(HttpConnectorFactory.class)) {
            port = connector.getPort();
        }
        return port;
    }

    public HealthCheck.Result getHealth() {
        return health;
    }

    @Override
    public String getName() {
        return "ServiceDiscoveryRegistrar";
    }
}
