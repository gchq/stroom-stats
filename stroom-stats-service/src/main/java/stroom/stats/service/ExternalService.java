package stroom.stats.service;

public enum ExternalService {
    //TODO think index can be removed
    INDEX ("stroomIndex"),
    AUTHENTICATION ("authentication");

    private final String name;

    ExternalService(String name){
        this.name = name;
    }

    public String getName() {
        return name;
    }
}