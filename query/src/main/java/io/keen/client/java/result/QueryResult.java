package io.keen.client.java.result;

import java.util.List;
import java.util.Map;

public abstract class QueryResult {

    public boolean isDouble() {
        return false;
    }

    public boolean isLong() {
        return false;
    }

    public boolean isString() {
        return false;
    }

    public boolean isListResult() {
        return false;
    }

    public boolean isIntervalResult() {
        return false;
    }

    public boolean isGroupResult() {
        return false;
    }

    public double doubleValue() {
        throw new IllegalStateException();
    }

    public long longValue() {
        throw new IllegalStateException();
    }

    public String stringValue() {
        throw new IllegalStateException();
    }

    public List<QueryResult> getListResults() {
        throw new IllegalStateException();
    }

    public Map<AbsoluteTimeframe, QueryResult> getIntervalResults() {
        throw new IllegalStateException();
    }

    public Map<Group, QueryResult> getGroupResults() {
        throw new IllegalStateException();
    }

}
