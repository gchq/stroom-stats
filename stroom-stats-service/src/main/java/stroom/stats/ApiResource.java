/*
 * Copyright 2017 Crown Copyright
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to the Free Software Foundation, Inc., 59
 * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 *
 */

package stroom.stats;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import io.dropwizard.hibernate.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.SearchRequest;
import stroom.stats.schema.Statistics;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class ApiResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApiResource.class);
    private HBaseClient hBaseClient;

    public ApiResource(HBaseClient hBaseClient) {
        this.hBaseClient = hBaseClient;
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
        return Response.accepted(hBaseClient.query(searchRequest)).build();
    }


    //TODO need an endpoint for completely purging a whole set of stats (passing in a stat data source uuid)

    //TODO need an endpoint for purging all stats to the configured retention periods
}