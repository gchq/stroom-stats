/*
 * Copyright 2017 Crown Copyright
 *
 * This file is part of Stroom-Stats.
 *
 * Stroom-Stats is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Stroom-Stats is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Stroom-Stats.  If not, see <http://www.gnu.org/licenses/>.
 */

package stroom.stats;

import com.codahale.metrics.annotation.Timed;
import com.codahale.metrics.health.HealthCheck;
import io.dropwizard.auth.Auth;
import io.dropwizard.hibernate.UnitOfWork;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.DocRef;
import stroom.query.api.SearchRequest;
import stroom.stats.schema.Statistics;

import javax.inject.Inject;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Optional;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class ApiResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApiResource.class);
    private HBaseClient hBaseClient;
    private ServiceDiscoveryManager serviceDiscoveryManager;
    private static final String NO_AUTHORISATION_SERVICE_MESSAGE
            = "I don't have an address for the Authorisation service, so I can't authorise requests!";

    @Inject
    public ApiResource(HBaseClient hBaseClient, ServiceDiscoveryManager serviceDiscoveryManager) {
        this.hBaseClient = hBaseClient;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
    }

    @GET
    @Timed
    public String home() {
        return "Welcome to the stroom-stats-service.";
    }

    @POST
    @Path("statistics")
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    @UnitOfWork
    public Response postStatistics(@Auth User user, @Valid Statistics statistics){
        LOGGER.debug("Received statistic");
        hBaseClient.addStatistics(statistics);
        return Response.accepted().build();
    }

    @POST
    @Path("search")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    @UnitOfWork
    public Response postQueryData(@Auth User user, @Valid SearchRequest searchRequest){
        LOGGER.debug("Received search request");

        Optional<String> authorisationServiceAddress = serviceDiscoveryManager.getAddress(ExternalServices.AUTHORISATION);
        if(authorisationServiceAddress.isPresent()){
            String authorisationUrl = String.format(
                    "%s/api/authorisation/isAuthorised",
                    authorisationServiceAddress.get());

            boolean isAuthorised = checkPermissions(authorisationUrl, user, searchRequest.getQuery().getDataSource());
            if(!isAuthorised){
                return Response
                        .status(Response.Status.UNAUTHORIZED)
                        .entity("User is not authorised to perform this action.")
                        .build();
            }
        } else {
            LOGGER.error(NO_AUTHORISATION_SERVICE_MESSAGE);
            return Response
                    .serverError()
                    .entity("This request cannot be authorised because the authorisation service (Stroom) is not available.")
                    .build();
        }


        return Response.accepted(hBaseClient.query(searchRequest)).build();
    }

    private boolean checkPermissions(String authorisationUrl, User user, DocRef statisticRef){
        Client client = ClientBuilder.newClient(new ClientConfig().register(ClientResponse.class));

        AuthorisationRequest authorisationRequest = new AuthorisationRequest(statisticRef, "USE");
        Response response = client
                .target(authorisationUrl)
                .request()
                .header("Authorization", "Bearer " + user.getJwt())
                .post(Entity.json(authorisationRequest));

        boolean isAuthorised = response.getStatus() == 200;
        return isAuthorised;
    }


    public HealthCheck.Result getHealth(){
        if(serviceDiscoveryManager.getAddress(ExternalServices.AUTHORISATION).isPresent()){
            return HealthCheck.Result.healthy();
        }
        else{
            return HealthCheck.Result.unhealthy(NO_AUTHORISATION_SERVICE_MESSAGE);
        }
    }

    //TODO need an endpoint for completely purging a whole set of stats (passing in a stat data source uuid)

    //TODO need an endpoint for purging all stats to the configured retention periods
}