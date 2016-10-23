package io.keen.client.java;

import java.util.HashMap;

/**
 * Class which represents a query filter
 * 
 * @author baumatron
 */
public class Filter extends RequestParameter {
    
    // Required parameters
    private final String propertyName;
    private final FilterOperator operator;
    private final Object propertyValue;
    
    public Filter(String propertyName, FilterOperator operator, Object propertyValue) {
        
        if (null == propertyName || propertyName.trim().isEmpty())
        {
            throw new IllegalArgumentException("Filter parameter 'propertyName' must be provided.");
        }
        
        if (null == operator)
        {
            throw new IllegalArgumentException("Filter parameter 'operator' must be provided.");
        }
        
        if (null == propertyValue)
        {
            throw new IllegalArgumentException("Filter parameter 'propertyValue' must be provided.");
        }
                
        this.propertyName = propertyName;
        this.operator = operator;
        this.propertyValue = propertyValue;
    }
    
    /**
     * Constructs request sub-parameters for the filter.
     * 
     * @return A jsonifiable object
     */
    @Override
    Object constructParameterRequestArgs() {
        
        HashMap<String, Object> args = new HashMap<String, Object>();
        
        args.put(KeenQueryConstants.PROPERTY_NAME, propertyName);
        args.put(KeenQueryConstants.OPERATOR, operator.toString());
        args.put(KeenQueryConstants.PROPERTY_VALUE, propertyValue);
        
        return args;
    }
}
