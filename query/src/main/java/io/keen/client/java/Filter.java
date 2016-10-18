/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.keen.client.java;

import java.util.HashMap;

/**
 * Class which represents a query filter
 * @author baumatron
 */
public class Filter implements RequestParameter {
    
    // Required parameters
    final public String propertyName;
    final public FilterOperator operator;
    final public Object propertyValue;
    
    public Filter(String propertyName, FilterOperator operator, Object propertyValue)
    {
        if (null == propertyName || propertyName.isEmpty())
        {
            throw new IllegalArgumentException("Filter parameter propertyName must be provided.");
        }
        
        if (null == operator)
        {
            throw new IllegalArgumentException("Filter parameter operator must be provided.");
        }
        
        if (null == propertyValue)
        {
            throw new IllegalArgumentException("Filter parameter propertyValue must be provided.");
        }
                
        this.propertyName = propertyName;
        this.operator = operator;
        this.propertyValue = propertyValue;
    }
    
    /**
     * Constructs request sub-parameters for the filter.
     * @return A jsonifiable object
     */
    @Override
    public Object constructParameterRequestArgs()
    {
        HashMap<String, Object> args = new HashMap<String, Object>();
        
        args.put(KeenQueryConstants.PROPERTY_NAME, propertyName);
        args.put(KeenQueryConstants.OPERATOR, operator.toString());
        args.put(KeenQueryConstants.PROPERTY_VALUE, propertyValue);
        
        return args;
    }
}
