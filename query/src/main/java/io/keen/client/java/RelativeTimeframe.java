package io.keen.client.java;

import java.util.HashMap;
import java.util.Map;

/**
 * Relative Timeframe, as specified by API docs: https://keen.io/docs/api/#relative-timeframes
 *
 * Created by claireyoung on 7/6/15.
 * @author claireyoung
 * @since 1.0.0
 */
public class RelativeTimeframe implements Timeframe {

    private final String relativeTimeframe;
    private final String timezone;

    /**
     * Construct a RelativeTimeframe with a specified relative time window.
     * @param relativeTimeframe the relative timeframe string, as specified by the API docs.
     *                          For example, "this_minute", "this_month", "this_year", etc.
     */
    public RelativeTimeframe(String relativeTimeframe) {
        this(relativeTimeframe, null);
    }
    
    /**
     * Construct a RelativeTimeframe with a specified relative time window, along with a
     * timezone to use as an offset.
     * @param relativeTimeframe the relative timeframe string, as specified by the API docs.
     *                          For example, "this_minute", "this_month", "this_year", etc.
     * @param timezone the timezone offset to use with the relative timeframe
     */
    public RelativeTimeframe(final String relativeTimeframe, final String timezone) {
        if (null == relativeTimeframe || relativeTimeframe.trim().isEmpty()) {
            throw new IllegalArgumentException("'relativeTimeframe' argument must be specified and not empty.");
        }
        
        this.relativeTimeframe = relativeTimeframe;
        if (null != timezone && !timezone.trim().isEmpty()) {
            this.timezone = timezone;
        }
        else {
            this.timezone = null;
        }
    }

    /**
     * Get the relative timeframe.
     *
     * @return  the relative timeframe.
     */
    public String getTimeframe() {
        return this.relativeTimeframe;
    }

    /**
     * Construct the Timeframe map to send in the query.
     *
     * @return  the Timeframe Json map to send in the query.
     */
    @Override
    public Map<String, Object> constructTimeframeArgs() {
        Map<String, Object> timeframe = new HashMap<String, Object>(3);
        
        if (null != this.relativeTimeframe) {
            timeframe.put(KeenQueryConstants.TIMEFRAME, this.relativeTimeframe);
        }
        
        if (null != this.timezone) {
            timeframe.put(KeenQueryConstants.TIMEZONE, this.timezone);
        }

        return timeframe;
    }
}
