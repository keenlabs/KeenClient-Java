package io.keen.client.java;

import java.util.HashMap;
import java.util.Map;

/**
 * Relative Timeframe, as specified by API docs: https://keen.io/docs/api/#relative-timeframes
 *
 * Created by claireyoung on 7/6/15.
 */
class RelativeTimeframe implements Timeframe {

    private final String relativeTimeframe;

    /**
     * Construct an AbsoluteTimeframe with a specified start and end date/time.
     * @param relativeTimeframe the relative timeframe string, as specified by the API docs.
     *                          For example, "this_minute", "this_month", "this_year", etc.
     */
    public RelativeTimeframe(String relativeTimeframe) {
        this.relativeTimeframe = relativeTimeframe;
    }

    /**
     * Get the relative timeframe.
     *
     * @return  the relative timeframe.
     */
    public String getTimeframe() {
        return relativeTimeframe;
    }

    /**
     * Construct the Timeframe map to send in the query.
     *
     * @return  the Timeframe Json map to send in the query.
     */
    @Override
    public Map<String, Object> constructTimeframeArgs() {
        Map timeframe = new HashMap<String, Object>();
        if (relativeTimeframe != null) {
            timeframe.put(KeenQueryConstants.TIMEFRAME, relativeTimeframe);
            return timeframe;
        }

        return null;
    }
}
