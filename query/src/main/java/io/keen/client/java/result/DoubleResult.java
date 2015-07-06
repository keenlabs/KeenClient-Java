package io.keen.client.java.result;

/**
 * Created by claireyoung on 7/6/15.
 */
public class DoubleResult extends QueryResult {
    private final double result;

    public DoubleResult(double result) {
        this.result = result;
    }

    @Override
    public boolean isDouble() {
        return true;
    }

    @Override
    public double doubleValue() {
        return result;
    }
}
