package io.keen.client.java;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by claireyoung on 7/6/15.
 */
public class AbsoluteTimeframe implements Timeframe {
    private String start;
    private String end;

    public AbsoluteTimeframe(String start, String end) {
        this.start = start;
        this.end = end;
    }

    public String getStart() { return start;}

    public String getEnd() {return end;}

    public Map<String, Object> constructTimeframeArgs() {
        Map timeframe = null;
        if (start != null && end != null) {
            Map absoluteTimeframe = new HashMap<String, Object>();
            absoluteTimeframe.put(KeenQueryConstants.START, start);
            absoluteTimeframe.put(KeenQueryConstants.END, end);

            timeframe = new HashMap<String, Object>();
            timeframe.put(KeenQueryConstants.TIMEFRAME, absoluteTimeframe);
            return timeframe;
        }
        return timeframe;
    }
}
