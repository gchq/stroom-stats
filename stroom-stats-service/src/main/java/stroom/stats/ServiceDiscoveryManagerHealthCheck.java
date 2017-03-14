package stroom.stats;

import com.codahale.metrics.health.HealthCheck;
import org.apache.curator.x.discovery.ServiceInstance;

import javax.inject.Inject;

public class ServiceDiscoveryManagerHealthCheck {

    private ServiceDiscoveryManager serviceDiscoveryManager;

    @Inject
    public ServiceDiscoveryManagerHealthCheck(ServiceDiscoveryManager serviceDiscoveryManager){

        this.serviceDiscoveryManager = serviceDiscoveryManager;
    }

    public HealthCheck.Result getKafkaHealth() {
        return check(serviceDiscoveryManager.getKafka(), "Kafka");

    }

    public HealthCheck.Result getHBaseHealth() {
        return check(serviceDiscoveryManager.getHBase(), "HBase");

    }

    public HealthCheck.Result getStroomDBHealth() {
        return check(serviceDiscoveryManager.getStroomDB(), "stroom-db");
    }

    private HealthCheck.Result check(
            ServiceInstance<String> serviceInstance,
            String serviceInstanceName){
        if(serviceInstance == null){
            return HealthCheck.Result.unhealthy(String.format(
                    "There are no registered instances of %s for me to use!",
                    serviceInstanceName));
        }
        else{
            return HealthCheck.Result.healthy(String.format(
                    "Found a %s instance for me to use at %s:%s.",
                    serviceInstanceName,
                    serviceInstance.getAddress(),
                    serviceInstance.getPort()));
        }
    }


}
