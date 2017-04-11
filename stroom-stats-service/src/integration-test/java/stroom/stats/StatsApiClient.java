package stroom.stats;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.Serializable;
import java.util.function.Supplier;

public class StatsApiClient {
    // Optional
    private AuthHeader authHeader = AuthHeader.VALID;
    private String mediaType = MediaType.APPLICATION_JSON;

    // Mandatory
    private Client client = null;
    private Supplier<Serializable> body;

    private String url;


    public StatsApiClient body(Supplier<Serializable> body){
        this.body = body;
        return this;
    }

    /**
     * Defaults to valid
     */
    public StatsApiClient authHeader(AuthHeader authHeader){
        this.authHeader = authHeader;
        return this;
    }

    public StatsApiClient useXml(){
        this.mediaType = MediaType.APPLICATION_XML;
        return this;
    }

    /**
     * This is the default MediaType.
     */
    public StatsApiClient useJson(){
        this.mediaType = MediaType.APPLICATION_JSON;
        return this;
    }

    public StatsApiClient client(Client client){
        this.client = client;
        return this;
    }

    public Response postStats(){
        this.url = AbstractAppIT.STATISTICS_URL;
        //TODO validations
        switch(mediaType){
            case MediaType.APPLICATION_JSON: return postJson();
            case MediaType.APPLICATION_XML: return postXml();
            default: throw new RuntimeException("Unsupported media type: " + mediaType);
        }
    }

    public Response getStats(){
        this.url = AbstractAppIT.QUERY_URL;
        //TODO validations
        switch(mediaType){
            case MediaType.APPLICATION_JSON: return postJson();
            case MediaType.APPLICATION_XML: return postXml();
            default: throw new RuntimeException("Unsupported media type: " + mediaType);
        }
    }

    private Response postJson(){
        Response response = client.target(url)
                .request()
                .header("Authorization", authHeader.get())
                .post(Entity.json(body.get()));
        return response;
    }

    private Response postXml(){
        Response response = client.target(url)
                .request()
                .header("Authorization", authHeader.get())
                .post(Entity.xml(body.get()));
        return response;
    }

    public void startProcessing() {
        postToTaskUrl(AbstractAppIT.START_PROCESSING_URL);
    }

    public void stopProcessing() {
        postToTaskUrl(AbstractAppIT.STOP_PROCESSING_URL);
    }

    private void postToTaskUrl(String url) {

        this.url = url;
        client.target(url)
                .request()
                .header("Authorization", authHeader.get())
                .post(null);
    }
}
