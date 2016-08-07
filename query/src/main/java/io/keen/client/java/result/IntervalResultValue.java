package io.keen.client.java.result;

import io.keen.client.java.AbsoluteTimeframe;

/**
 * Models a single piece of a full interval result.
 */
public class IntervalResultValue {

    private final AbsoluteTimeframe timeframe;
    private final QueryResult result;

    public IntervalResultValue(AbsoluteTimeframe timeframe, QueryResult result) {
        this.timeframe = timeframe;
        this.result = result;
    }

    public AbsoluteTimeframe getTimeframe() {
        return timeframe;
    }

    public QueryResult getResult() {
        return result;
    }
}
