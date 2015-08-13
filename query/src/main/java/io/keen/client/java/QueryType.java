package io.keen.client.java;

/**
 * QueryType specifies which query to run.
 *
 * Created by claireyoung on 6/16/15.
 * @author claireyoung
 * @since 1.0.0
 */
public enum QueryType {
    COUNT(KeenQueryConstants.COUNT),
    COUNT_UNIQUE(KeenQueryConstants.COUNT_UNIQUE),
    MINIMUM(KeenQueryConstants.MINIMUM),
    MAXIMUM(KeenQueryConstants.MAXIMUM),
    AVERAGE(KeenQueryConstants.AVERAGE),
    MEDIAN(KeenQueryConstants.MEDIAN),
    PERCENTILE(KeenQueryConstants.PERCENTILE_RESOURCE),
    SUM(KeenQueryConstants.SUM),
    SELECT_UNIQUE(KeenQueryConstants.SELECT_UNIQUE);

    private final String text;

    private QueryType(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
