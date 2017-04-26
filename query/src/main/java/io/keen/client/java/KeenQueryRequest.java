package io.keen.client.java;

import java.net.URL;
import java.util.Collection;
import java.util.Map;

import io.keen.client.java.exceptions.KeenQueryClientException;
import io.keen.client.java.http.HttpMethods;

/**
 * Interface to be implemented by a query request
 *
 * @author baumatron
 */
abstract class KeenQueryRequest {
    abstract URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId)
            throws KeenQueryClientException;

    // By default, we POST to get most of our query results.
    String getHttpMethod() {
        return HttpMethods.POST;
    }

    String getAuthKey(KeenProject project) {
        return project.getReadKey();
    }

    abstract Map<String, Object> constructRequestArgs();

    abstract boolean groupedResponseExpected();

    abstract boolean intervalResponseExpected();

    Collection<String> getGroupByParams() {
        throw new IllegalStateException(
                "Not all KeenQueryRequest subclasses necessarily can provide GroupBy parameters.");
    }
}
