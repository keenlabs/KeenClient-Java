package io.keen.client.java;

/**
 * Base class that represents requests to the endpoints for managing persistent analyses, such as
 * <a href="https://keen.io/docs/api/#saved-queries">Saved/Cached Queries</a> and
 * <a href="https://keen.io/docs/api/#cached-datasets">Cached Datasets</a>.
 *
 * Should be an interface, but KeenQueryRequest is meant to be a package-private interface defining
 * package-private functionality, so until the next major version when we fix up public interface
 * surface area, it's a package-private abstract class.
 *
 * @author masojus
 */
abstract class PersistentAnalysis extends KeenQueryRequest {
    private final String httpMethod;
    private final boolean needsMasterKey;

    // The name of the specific persistent analysis we're working on, if any--e.g. "max_signups".
    private final String resourceName;
    private final String displayName;

    // TODO : Validate query resource name. As per the docs, "Names of Saved Queries can only
    // contain alphanumeric characters, hyphens ( - ), and underscores ( _ )." Same goes for Cached
    // Dataset resource names.

    PersistentAnalysis(String httpMethod,
                       boolean needsMasterKey,
                       String resourceName,
                       String displayName) {
        this.httpMethod = httpMethod; // TODO : Validate??
        this.needsMasterKey = needsMasterKey;
        this.resourceName = resourceName; // TODO : Validate??
        this.displayName = displayName; // TODO : Validate?? Not null, empty or whitespace probably
    }

    String getResourceName() {
        return resourceName;
    }

    String getDisplayName() {
        return displayName;
    }

    /**
     * Does this represent a request to retrieve a result or results?
     *
     * @return True if retrieving a result, false otherwise.
     */
    boolean retrievingResults() {
        return false;
    }

    @Override
    String getHttpMethod() {
        return httpMethod;
    }

    @Override
    String getAuthKey(KeenProject project) {
        return (needsMasterKey ? project.getMasterKey() : project.getReadKey());
    }

    @Override
    boolean groupedResponseExpected() {
        return false;
    }

    @Override
    boolean intervalResponseExpected() {
        return false;
    }
}
