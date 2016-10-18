/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.keen.client.java;

/**
 *
 * @author baumatron
 */
public interface RequestParameter {
    
    /**
     * Returns a jsonifiable object, such as a Map or a List, for a request parameter.
     * @return A jsonifiable object, such as a Map or a List.
     */
    public Object constructParameterRequestArgs();
    
}
