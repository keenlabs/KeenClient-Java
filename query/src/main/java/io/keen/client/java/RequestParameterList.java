/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.keen.client.java;

import java.util.ArrayList;
import java.util.List;

/**
 * A helper class for dealing with lists of jsonifiable parameters.
 * Constructs a jsonifiable list of the jsonifiable parameters for each sub-parameter.
 * @author baumatron
 * @param <RequestParameterType> The type of RequestParameter
 */
public class RequestParameterList<RequestParameterType extends RequestParameter>
    implements RequestParameter {
    
    List<RequestParameterType> parameterList;
    
    public RequestParameterList(List<RequestParameterType> parameterList)
    {
        if (null == parameterList || parameterList.isEmpty())
        {
            throw new IllegalArgumentException("RequestParameterList parameterList is a required parameter and must not be empty.");
        }
        
        this.parameterList = parameterList;
    }

    @Override
    public Object constructParameterRequestArgs() {
        
        List<Object> requestArgList = new ArrayList<Object>();
        
        for (RequestParameterType parameter : this.parameterList)
        {
            requestArgList.add(parameter.constructParameterRequestArgs());
        }
        
        return requestArgList;
    }
}
