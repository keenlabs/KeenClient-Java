package io.keen.client.java;

/**
 * Exists solely to provide an abstraction around environment variables so we can actually test them.
 *
 * @since 1.0.0
 */
class Environment {

    /**
     * DOCUMENT
     *
     * @return
     */
    public String getKeenProjectId() {
        return getValue("KEEN_PROJECT_ID");
    }

    /**
     * DOCUMENT
     *
     * @return
     */
    public String getKeenWriteKey() {
        return getValue("KEEN_WRITE_KEY");
    }

    /**
     * DOCUMENT
     *
     * @return
     */
    public String getKeenReadKey() {
        return getValue("KEEN_READ_KEY");
    }

    /**
     * DOCUMENT
     *
     * @param name
     * @return
     */
    private String getValue(String name) {
        return System.getenv().get(name);
    }

}
