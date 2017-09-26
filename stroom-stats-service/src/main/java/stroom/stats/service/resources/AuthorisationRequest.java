package stroom.stats.service.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import stroom.query.api.v2.DocRef;

public class AuthorisationRequest {
    @JsonProperty
    private DocRef docRef;
    @JsonProperty
    private String permission;

    public AuthorisationRequest(DocRef docRef, String permission){
        this.docRef = docRef;
        this.permission = permission;
    }

    public DocRef getDocRef() {
        return docRef;
    }

    public String getPermission() {
        return permission;
    }
}
