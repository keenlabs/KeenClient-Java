package io.keen.client.java;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.HashMap;
import java.util.Map;

/**
 * Class which represents a query filter
 * 
 * @author baumatron, masojus
 */
public class Filter extends RequestParameter<Map<String, Object>> {

    // Required parameters
    private final String propertyName;
    private final FilterOperator operator;
    private final Object propertyValue;

    public Filter(String propertyName, FilterOperator operator, Object propertyValue) {
        if (null == propertyName || propertyName.trim().isEmpty()) {
            throw new IllegalArgumentException("Filter parameter 'propertyName' must be provided.");
        }

        if (null == operator) {
            throw new IllegalArgumentException("Filter parameter 'operator' must be provided.");
        }

        if (null == propertyValue) {
            throw new IllegalArgumentException("Filter parameter 'propertyValue' must be provided.");
        }

        this.propertyName = propertyName;
        this.operator = operator;
        this.propertyValue = propertyValue;
    }

    /**
     * Tells us if this Filter represents a <a href="https://keen.io/docs/api/#geo-filtering">Geo
     * Filter</a>.
     *
     * @return true if this is a Geo Filter.
     */
    public boolean isGeoFilter() {
        return FilterOperator.WITHIN == this.operator;
    }

    /**
     * Constructs request sub-parameters for the filter.
     * 
     * @return A jsonifiable object
     */
    @Override
    Map<String, Object> constructParameterRequestArgs() {
        Map<String, Object> args = new HashMap<String, Object>();

        args.put(KeenQueryConstants.PROPERTY_NAME, propertyName);
        args.put(KeenQueryConstants.OPERATOR, operator.toString());
        args.put(KeenQueryConstants.PROPERTY_VALUE, propertyValue);

        return args;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }
}
