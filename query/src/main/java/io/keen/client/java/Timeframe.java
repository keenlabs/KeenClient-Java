package io.keen.client.java;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by claireyoung on 6/12/15.
 */
public class Timeframe {

    private String relativeTimeframe;
    private AbsoluteTimeframe absoluteTimeframe;

    Timeframe(String relativeTimeframe) {
        absoluteTimeframe = null;
        this.relativeTimeframe = relativeTimeframe;
    }

    Timeframe(String start, String end) {
        relativeTimeframe = null;
        absoluteTimeframe = new AbsoluteTimeframe(start, end);
    }

    Timeframe(AbsoluteTimeframe absoluteTimeframe) {
        relativeTimeframe = null;
        this.absoluteTimeframe = absoluteTimeframe;
    }

    Map<String, Object> constructTimeframeArgs() {
        Map timeframe = new HashMap<String, Object>();
        if (relativeTimeframe != null) {
            timeframe.put(KeenQueryConstants.TIMEFRAME, relativeTimeframe);
            return timeframe;
        }
        if (absoluteTimeframe != null) {
            timeframe.put(KeenQueryConstants.TIMEFRAME, absoluteTimeframe.constructAbsoluteTimeframeArgs());
            return timeframe;
        }
        return null;
    }


    public class AbsoluteTimeframe {
        private String start;
        private String end;

        AbsoluteTimeframe(String start, String end) {
            this.start = start;
            this.end = end;
        }

        Map<String, Object> constructAbsoluteTimeframeArgs() {
            Map timeframe = new HashMap<String, Object>();
            timeframe.put(KeenQueryConstants.START, start);
            timeframe.put(KeenQueryConstants.END, end);
            return timeframe;
        }
    }
}
