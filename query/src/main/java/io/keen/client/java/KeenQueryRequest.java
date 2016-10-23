package io.keen.client.java;

import io.keen.client.java.exceptions.KeenQueryClientException;
import java.net.URL;
import java.util.Map;

/**
 * Interface to be implemented by a query request
 *
 * @author baumatron
 */
abstract class KeenQueryRequest {
    
    abstract URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId) throws KeenQueryClientException;
    
    abstract Map<String, Object> constructRequestArgs();
    
    abstract boolean groupedResponseExpected();
    
    abstract boolean intervalResponseExpected();
}
