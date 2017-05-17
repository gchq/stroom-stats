package stroom.stats;

public enum ExternalServices {
    HBASE ("hbase"),
    KAFKA ("kafka"),
    STROOM_DB ("stroom-db"),
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