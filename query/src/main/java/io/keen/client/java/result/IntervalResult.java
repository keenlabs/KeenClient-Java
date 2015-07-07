package io.keen.client.java.result;

import java.util.Map;
import java.util.Collections;

import io.keen.client.java.AbsoluteTimeframe;

/**
 * <p>IntervalResult is for if Interval properties were specified in the query.
 * If so, this object contains a Map that consist of </p>
 * <ul>
 * <li>AbsoluteTimeframe, which contains the timeframe as specified by the Interval</li>
 * <li>QueryResult, which is the result of the query as grouped by each timeframe</li>
 * </ul>
 *  *
 * Created by claireyoung on 7/6/15.
 */
public class IntervalResult extends QueryResult {

    private final Map<AbsoluteTimeframe, QueryResult> results;

    /**
     * @param results the result map of AbsoluteTimeframes to the QueryResult.
     */
    public IntervalResult(Map<AbsoluteTimeframe, QueryResult> results) {
        this.results = Collections.unmodifiableMap(results);
    }

    /**
     * @return  {@code true}
     */
    @Override
    public boolean isIntervalResult() {
        return true;
    }

    /**
     * @return map of AbsoluteTimeframe to QueryResult objects
     */
    @Override
    public Map<AbsoluteTimeframe, QueryResult> getIntervalResults() {
        return results;
    }

}
