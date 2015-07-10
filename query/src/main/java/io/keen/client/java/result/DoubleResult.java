package io.keen.client.java.result;

/**
 * DoubleResult is for if the QueryResult object is of type Double.
 *
 * Created by claireyoung on 7/6/15.
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
}
