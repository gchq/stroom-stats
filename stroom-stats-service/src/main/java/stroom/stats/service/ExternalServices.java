package stroom.stats.service;

public enum ExternalServices {
    INDEX ("index"),
    AUTHORISATION ("authorisation"),
    AUTHENTICATION ("authentication");

    private final String name;

    ExternalServices(String name){
        this.name = name;
    }

    public String getName() {
        return name;
    }
}