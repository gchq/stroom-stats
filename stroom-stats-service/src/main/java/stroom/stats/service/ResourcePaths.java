package stroom.stats.service;

public interface ResourcePaths {
    String ROOT_PATH = "/api";

    String STROOM_STATS = "/stroom-stats";

    String V1 = "/v1";
    String V2 = "/v2";
    String V3 = "/v3";

    static String removePathDelimeters(final String value) {
        if (value == null) {
            return null;
        }
        return value
                .replaceFirst("^/","")
                .replaceFirst("/$", "");
    }
}
