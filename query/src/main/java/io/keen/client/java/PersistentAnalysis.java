package io.keen.client.java;

import io.keen.client.java.http.HttpMethods;

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
    // This regex does not match strings like "árbol π" since the API only accepts ASCII in the URL.
    private static final String RESOURCE_NAME_REGEX = "^[\\w-]*$";

    private final String httpMethod;
    private final boolean needsMasterKey;

    // The name of the specific persistent analysis we're working on, if any--e.g. "max_signups".
    private final String resourceName;
    private final String displayName;


    PersistentAnalysis(String httpMethod,
                       boolean needsMasterKey,
                       String resourceName,
                       String displayName) {
        // HTTP Method is provided by our code, so let's trust it's valid.
        this.httpMethod = httpMethod;
        this.needsMasterKey = needsMasterKey;

        // The resource name can only be omitted for a GET request.
        if (null == resourceName && !HttpMethods.GET.equals(httpMethod)) {
            throw new IllegalArgumentException("A resource name is required if not a GET request.");
        }

        if (null != resourceName) {
            PersistentAnalysis.validateResourceName(resourceName);
        }

        this.resourceName = resourceName;

        // display name is optional, so if it's null that's OK, but if it isn't, validate it.
        if (null != displayName) {
            PersistentAnalysis.validateDisplayName(displayName);
        }

        this.displayName = displayName;
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

    static void validateResourceName(String resourceName) {
        // Validate the resource name. As per the docs, "Names of Saved Queries can only
        // contain alphanumeric characters, hyphens ( - ), and underscores ( _ )." Same goes for
        // Cached Dataset resource names.
        if (null == resourceName || !resourceName.matches(PersistentAnalysis.RESOURCE_NAME_REGEX)) {
            throw new IllegalArgumentException("The resource name can only be comprised of " +
                                               "alphanumerics, hyphens and underscores.");
        }
    }

    static void validateDisplayName(String displayName) {
        if (null == displayName || displayName.trim().isEmpty()) {
            throw new IllegalArgumentException("The display name cannot be null, empty or " +
                                               "whitespace only.");
        }
    }
}
