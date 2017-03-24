package stroom.stats;

public enum AuthHeader {
    VALID,
    INVALID,
    MISSING;

    public String get(){
        switch (this){
            case VALID: return AuthorizationHelper.getHeaderWithValidCredentials();
            case INVALID: return AuthorizationHelper.getHeaderWithInvalidCredentials();
            case MISSING: return AuthorizationHelper.getHeaderWithMissingCredentials();
            default: return AuthorizationHelper.getHeaderWithValidCredentials();
        }
    }
}
