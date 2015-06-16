package io.keen.client.java;

/**
 * Created by claireyoung on 6/15/15.
 */
public class Interval {
    private Timeframe timeframe;
    private QueryResult value;

    Interval(Timeframe timeframe, QueryResult value) {
        this.timeframe = timeframe;
        this.value = value;
    }

    Timeframe getTimeframe() {
        return timeframe;
    }

    QueryResult getValue() {
        return value;
    }
}
