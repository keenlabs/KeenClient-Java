package io.keen.client.java;

import java.util.HashMap;

/**
 * Created by claireyoung on 6/15/15.
 */
public class GroupByResult extends QueryResult {

    protected HashMap<String, Object> properties;

    protected GroupByResult(HashMap<String, Object> properties, QueryResult value) {
        super(value);
        this.properties = properties;
    }

    public HashMap<String, Object> getProperties() {
        return this.properties;
    }
}
