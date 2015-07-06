package io.keen.client.java.result;

import java.util.Map;
import java.util.Collections;

import io.keen.client.java.AbsoluteTimeframe;

/**
 * Created by claireyoung on 7/6/15.
 */
public class IntervalResult extends QueryResult {

    private final Map<AbsoluteTimeframe, QueryResult> results;

    public IntervalResult(Map<AbsoluteTimeframe, QueryResult> results) {
        this.results = Collections.unmodifiableMap(results);
    }

    @Override
    public boolean isIntervalResult() {
        return true;
    }

    @Override
    public Map<AbsoluteTimeframe, QueryResult> getIntervalResults() {
        return results;
    }

}
