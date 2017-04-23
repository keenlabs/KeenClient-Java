package io.keen.client.java.result;

import java.util.Collections;
import java.util.List;

/**
 * <p>IntervalResult is for if Interval properties were specified in the query.
 *
 * @author claireyoung
 * @since 1.0.0, 07/06/15
 */
public class IntervalResult extends QueryResult {
    private final List<IntervalResultValue> results;

    /**
     * @param results the result List of IntervalResultValues
     */
    public IntervalResult(List<IntervalResultValue> results) {
        this.results = Collections.unmodifiableList(results);
    }

    /**
     * @return  {@code true}
     */
    @Override
    public boolean isIntervalResult() {
        return true;
    }

    /**
     * @return list of IntervalResultValues
     */
    @Override
    public List<IntervalResultValue> getIntervalResults() {
        return results;
    }

}
