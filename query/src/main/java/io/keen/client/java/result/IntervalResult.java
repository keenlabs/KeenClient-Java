package io.keen.client.java.result;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Collections;
import java.util.List;

/**
 * <p>IntervalResult is for if Interval properties were specified in the query.
 *
 * @author claireyoung
 * @since 1.0.0, 07/06/15
 */
public class IntervalResult extends QueryResult {
    private final List<IntervalResultValue> results;

    /**
     * @param results the result List of IntervalResultValues
     */
    public IntervalResult(List<IntervalResultValue> results) {
        this.results = Collections.unmodifiableList(results);
    }

    /**
     * @return  {@code true}
     */
    @Override
    public boolean isIntervalResult() {
        return true;
    }

    /**
     * @return list of IntervalResultValues
     */
    @Override
    public List<IntervalResultValue> getIntervalResults() {
        return results;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }
}
