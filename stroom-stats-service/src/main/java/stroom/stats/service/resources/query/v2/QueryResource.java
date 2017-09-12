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

package stroom.stats.service.resources.query.v2;

import com.codahale.metrics.annotation.Timed;
import com.codahale.metrics.health.HealthCheck;
import io.dropwizard.hibernate.UnitOfWork;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.QueryKey;
import stroom.query.api.v2.SearchRequest;
import stroom.stats.HBaseClient;
import stroom.stats.datasource.DataSourceService;
import stroom.stats.service.ExternalService;
import stroom.stats.service.ResourcePaths;
import stroom.stats.service.ServiceDiscoverer;
import stroom.stats.service.auth.User;
import stroom.stats.service.resources.AuthorisationRequest;
import stroom.stats.util.healthchecks.HasHealthCheck;

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
import java.util.function.Supplier;

@Path(ResourcePaths.ROOT_PATH + ResourcePaths.STROOM_STATS + ResourcePaths.V2)
@Produces(MediaType.APPLICATION_JSON)
public class QueryResource implements HasHealthCheck {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResource.class);

    public static final String DATA_SOURCE_ENDPOINT = "/dataSource";
    public static final String SEARCH_ENDPOINT = "/search";
    public static final String DESTROY_ENDPOINT = "/destroy";

    private final HBaseClient hBaseClient;
    private final DataSourceService dataSourceService;
    private final ServiceDiscoverer serviceDiscoverer;
    private static final String NO_AUTHORISATION_SERVICE_MESSAGE
            = "I don't have an address for the Authorisation service, so I can't authorise requests!";

    @Inject
    public QueryResource(final HBaseClient hBaseClient,
                         final DataSourceService dataSourceService,
                         final ServiceDiscoverer serviceDiscoverer) {
        this.hBaseClient = hBaseClient;
        this.dataSourceService = dataSourceService;
        this.serviceDiscoverer = serviceDiscoverer;
    }

    @GET
    @Timed
    public String home() {
        return "Welcome to stroom-stats.";
    }

//    @POST
//    @Path("statistics")
//    @Consumes(MediaType.APPLICATION_XML)
//    @Produces(MediaType.APPLICATION_JSON)
//    @Timed
//    @UnitOfWork
//    public Response postStatistics(@Auth User user, @Valid Statistics statistics){
//        LOGGER.debug("Received statistic");
//        hBaseClient.addStatistics(statistics);
//        return Response.accepted().build();
//    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path(DATA_SOURCE_ENDPOINT)
    @Timed
//    public Response getDataSource(@Auth User user, @Valid final DocRef docRef) {
        public Response getDataSource(@Valid final DocRef docRef) {

//        return performWithAuthorisation(user,
        return performWithAuthorisation(null,
                docRef,
                () -> dataSourceService.getDatasource(docRef)
                        .map(dataSource -> Response.ok(dataSource).build())
                        .orElse(Response.noContent().build()));
    }

    @POST
    @Path(SEARCH_ENDPOINT)
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    @UnitOfWork
//    public Response search(@Auth User user, @Valid SearchRequest searchRequest){
    public Response search(@Valid SearchRequest searchRequest){
        LOGGER.debug("Received search request");

//        return performWithAuthorisation(user,
        return performWithAuthorisation(null,
                searchRequest.getQuery().getDataSource(),
                () -> Response.ok(hBaseClient.query(searchRequest)).build());
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path(DESTROY_ENDPOINT)
    @Timed
    public Response destroy(@Valid final QueryKey queryKey) {
//    public Response destroy(@Auth User user, @Valid final QueryKey queryKey) {

        //destroy does nothing on stroom-stats as we don't hold any query state
        //If we return a failure response then stroom will error so just silently
        //return a 200
        return Response
                .ok()
                .build();
    }

    private Response performWithAuthorisation(final User user, final DocRef docRef, final Supplier<Response> responseProvider) {

        Optional<String> authorisationServiceAddress = serviceDiscoverer.getServiceInstanceAddress(ExternalService.AUTHORISATION);

        if(authorisationServiceAddress.isPresent()){
            String authorisationUrl = String.format(
//                    "%s/api/authorisation/isAuthorised",
                    "%s/isAuthorised",
                    authorisationServiceAddress.get());

            boolean isAuthorised = checkPermissions(authorisationUrl, user, docRef);
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

        try {
            return responseProvider.get();
        } catch (Exception e) {
            LOGGER.error("Error processing web service request",e);
            return Response
                    .serverError()
                    .entity("Unexpected error processing request, check the server logs")
                    .build();
        }
    }

    private boolean checkPermissions(String authorisationUrl, User user, DocRef statisticRef){
        Client client = ClientBuilder.newClient(new ClientConfig().register(ClientResponse.class));

        if (user != null) {
            AuthorisationRequest authorisationRequest = new AuthorisationRequest(statisticRef, "USE");
            Response response = client
                    .target(authorisationUrl)
                    .request()
                    .header("Authorization", "Bearer " + user.getJwt())
                    .post(Entity.json(authorisationRequest));

            boolean isAuthorised = response.getStatus() == 200;
            return isAuthorised;
        } else {
            return true;
        }
    }


    @Override
    public HealthCheck.Result getHealth(){
        if(serviceDiscoverer.getServiceInstance(ExternalService.AUTHORISATION).isPresent()){
            return HealthCheck.Result.healthy();
        } else {
            return HealthCheck.Result.unhealthy(NO_AUTHORISATION_SERVICE_MESSAGE);
        }
    }

    //TODO need an endpoint for completely purging a whole set of stats (passing in a stat data source uuid)

    //TODO need an endpoint for purging all stats to the configured retention periods
}