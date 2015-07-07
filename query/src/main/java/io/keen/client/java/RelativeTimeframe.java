package io.keen.client.java;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by claireyoung on 7/6/15.
 */
class RelativeTimeframe implements Timeframe {

    private String relativeTimeframe;

    public RelativeTimeframe(String relativeTimeframe) {
        this.relativeTimeframe = relativeTimeframe;
    }

    public String getTimeframe() {
        return relativeTimeframe;
    }

    public Map<String, Object> constructTimeframeArgs() {
        Map timeframe = new HashMap<String, Object>();
        if (relativeTimeframe != null) {
            timeframe.put(KeenQueryConstants.TIMEFRAME, relativeTimeframe);
            return timeframe;
        }

        return null;
    }
}
