package io.keen.client.java.result;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Group is an object that contains the Group By properties and the value of the properties.
 *
 * @author claireyoung
 * @since 1.0.0, 07/06/15
 */
public class Group {
    private final Map<String, Object> properties;

    /**
     * @param properties The map of properties to property values.
     */
    public Group(Map<String, Object> properties) {
        this.properties = Collections.unmodifiableMap(properties);
    }

    /**
     * @return properties The map of properties to property values.
     */
    public Map<String, Object> getProperties() {
        return properties;
    }

    /**
     * @return property names.
     */
    public Set<String> getPropertyNames() {
        return properties.keySet();
    }

    /**
     * @param propertyName property name.
     * @return the property value.
     */
    public Object getGroupValue(String propertyName) {
        if (!properties.containsKey(propertyName)) {
            throw new IllegalStateException(
                    "GroupBy does not contain expected property '" + propertyName + "'.");
        }

        return properties.get(propertyName);
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
