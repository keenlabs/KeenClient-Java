package io.keen.client.java.result;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * DoubleResult is for if the QueryResult object is of type Double.
 *
 * @author claireyoung
 * @since 1.0.0, 07/06/15
 */
public class DoubleResult extends QueryResult {
    private final double result;

    /**
     * @param result the result.
     */
    public DoubleResult(double result) {
        this.result = result;
    }

    /**
     * @return  {@code true}
     */
    @Override
    public boolean isDouble() {
        return true;
    }

    /**
     * @return double value
     */
    @Override
    public double doubleValue() {
        return result;
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
