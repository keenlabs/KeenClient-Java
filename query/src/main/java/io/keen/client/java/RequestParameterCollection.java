package io.keen.client.java;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * A helper class for dealing with collections of jsonifiable parameters.
 * Constructs a jsonifiable collection of the jsonifiable parameters for each sub-parameter.
 * 
 * @author baumatron, masojus
 * @param <RequestParameterT> The type of RequestParameter
 */
class RequestParameterCollection<RequestParameterT extends RequestParameter<?>>
    extends RequestParameter<Collection<Object>>
    implements Iterable<RequestParameterT> {
    
    private Collection<? extends RequestParameterT> parameters;
    
    RequestParameterCollection(Collection<? extends RequestParameterT> parameters) {
        if (null == parameters || parameters.isEmpty()) {
            throw new IllegalArgumentException("'parameters' is a required parameter and must not be empty.");
        }
        
        this.parameters = parameters;
    }

    @Override
    Collection<Object> constructParameterRequestArgs() {
        // Each of these contained Object instances is generally either another Collection<?> or
        // a Map<String, Object> depending upon whether the corresponding JSON should be a nested
        // object or a JSON array of objects/values.
        Collection<Object> requestArgList = new LinkedList<Object>();
        
        for (RequestParameterT parameter : this.parameters) {
            requestArgList.add(parameter.constructParameterRequestArgs());
        }
        
        return requestArgList;
    }

    @Override
    public Iterator<RequestParameterT> iterator() {
        Collection<RequestParameterT> parametersIterable =
                Collections.unmodifiableCollection(this.parameters);

        return parametersIterable.iterator();
    }
}
