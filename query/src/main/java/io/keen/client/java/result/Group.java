package io.keen.client.java.result;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class Group {

    private final Map<String, Object> properties;

    public Group(Map<String, Object> properties) {
        this.properties = Collections.unmodifiableMap(properties);
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public Set<String> getPropertyNames() {
        return properties.keySet();
    }

    public Object getGroupValue(String propertyName) {
        // TODO: Validate that propertyName is present here, to avoid NPE's up the stack.
        return properties.get(propertyName);
    }
}
