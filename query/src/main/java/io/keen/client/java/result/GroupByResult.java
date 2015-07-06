package io.keen.client.java.result;

import java.util.Collections;
import java.util.Map;

public class GroupByResult extends QueryResult {

    private final Map<Group, QueryResult> results;

    public GroupByResult(Map<Group, QueryResult> results) {
        this.results = Collections.unmodifiableMap(results);
    }

    @Override
    public boolean isGroupResult() {
        return true;
    }

    @Override
    public Map<Group, QueryResult> getGroupResults() {
        return results;
    }

}
