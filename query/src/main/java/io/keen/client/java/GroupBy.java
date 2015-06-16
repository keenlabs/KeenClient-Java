package io.keen.client.java;

import java.util.HashMap;

/**
 * Created by claireyoung on 6/15/15.
 */
public class GroupBy {

    private HashMap<String, QueryResult> properties;
    private QueryResult result;

    GroupBy(HashMap<String, QueryResult> properties, QueryResult groupByResult) {
        this.properties = properties;
        this.result = groupByResult;
    }

    HashMap<String, QueryResult> getProperties() {
        return this.properties;
    }

    QueryResult getResult() {
        return this.result;
    }
}
