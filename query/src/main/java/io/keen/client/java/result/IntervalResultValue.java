package io.keen.client.java.result;

import io.keen.client.java.AbsoluteTimeframe;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

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

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
    }
}
