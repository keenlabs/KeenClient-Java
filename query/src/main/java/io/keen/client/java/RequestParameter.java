package io.keen.client.java;

/**
 * An interface implemented by a request parameter object.
 *
 * @author baumatron
 */
abstract class RequestParameter<ArgsT> {
    
    /**
     * Returns a jsonifiable object, such as a Map or a List, for a request parameter.
     * 
     * @return A jsonifiable object, such as a Map or a List.
     */
    abstract ArgsT constructParameterRequestArgs();
}
