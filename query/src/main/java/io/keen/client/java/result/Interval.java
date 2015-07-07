package io.keen.client.java.result;


import io.keen.client.java.AbsoluteTimeframe;

/**
 * Created by claireyoung on 7/6/15.
 */
public class Interval {
    private final AbsoluteTimeframe timeframe;

    public Interval(String start, String end) {
        timeframe = new AbsoluteTimeframe(start, end);
    }

    public String getStart()  {
        return timeframe.getStart();
    }

    public String getEnd()  {
        return timeframe.getEnd();
    }

}
