package io.keen.client.java;

/**
 * Created by claireyoung on 6/15/15.
 */
public class IntervalResult extends QueryResult {
    private Timeframe timeframe;

    protected IntervalResult(Timeframe timeframe, QueryResult value) {
        super(value);
        this.timeframe = timeframe;
    }

    public Timeframe getTimeframe() {
        return timeframe;
    }
}
