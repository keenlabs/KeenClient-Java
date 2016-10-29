package io.keen.client.java;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>AbsoluteTimeframe allows users to construct a Timeframe with
 * start and end dates and times. Please refer to: https://keen.io/docs/api/#absolute-timeframes</p>
 *
 * @author claireyoung
 * @since 1.0.0
 *
 */
public class AbsoluteTimeframe implements Timeframe {
    private final String start;
    private final String end;

    /**
     * Construct an AbsoluteTimeframe with a specified start and end date/time.
     * @param start the start time
     * @param end the end time
     */
    public AbsoluteTimeframe(String start, String end) {
        this.start = start;
        this.end = end;
    }

    /**
     * Get the Start date/time timeframe.
     *
     * @return  the start date/time.
     */
    public String getStart() { return start; }

    /**
     * Get the End date/time timeframe.
     *
     * @return  the end date/time.
     */
    public String getEnd() { return end; }

    /**
     * Construct the Timeframe map to send in the query.
     *
     * @return  the Timeframe Json map to send in the query.
     */
    @Override
    public Map<String, Object> constructTimeframeArgs() {
        Map<String, Object> timeframe = null;

        if (start != null && end != null) {
            Map<String, Object> absoluteTimeframe = new HashMap<String, Object>(3);

            absoluteTimeframe.put(KeenQueryConstants.START, start);
            absoluteTimeframe.put(KeenQueryConstants.END, end);

            timeframe = new HashMap<String, Object>(2);
            timeframe.put(KeenQueryConstants.TIMEFRAME, absoluteTimeframe);

            return timeframe;
        }

        return timeframe;
    }
}
