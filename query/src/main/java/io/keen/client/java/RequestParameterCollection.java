package io.keen.client.java;

import java.util.LinkedList;
import java.util.Collection;

/**
 * A helper class for dealing with collections of jsonifiable parameters.
 * Constructs a jsonifiable collection of the jsonifiable parameters for each sub-parameter.
 * 
 * @author baumatron
 * @param <RequestParameterT> The type of RequestParameter
 */
class RequestParameterCollection<RequestParameterT extends RequestParameter>
    extends RequestParameter {
    
    private Collection<? extends RequestParameterT> parameters;
    
    RequestParameterCollection(Collection<? extends RequestParameterT> parameters) {
        if (null == parameters || parameters.isEmpty()) {
            throw new IllegalArgumentException("'parameters' is a required parameter and must not be empty.");
        }
        
        this.parameters = parameters;
    }

    @Override
    Object constructParameterRequestArgs() {
        Collection<Object> requestArgList = new LinkedList<Object>();
        
        for (RequestParameterT parameter : this.parameters) {
            requestArgList.add(parameter.constructParameterRequestArgs());
        }
        
        return requestArgList;
    }
}
