package io.keen.client.java;

/**
 * Exists solely to provide an abstraction around environment variables so we can actually test them.
 */
class Environment {

    public String getKeenProjectId() {
        return getValue("KEEN_PROJECT_ID");
    }

    public String getKeenWriteKey() {
        return getValue("KEEN_WRITE_KEY");
    }

    public String getKeenReadKey() {
        return getValue("KEEN_READ_KEY");
    }

    private String getValue(String name) {
        return System.getenv().get(name);
    }

}
