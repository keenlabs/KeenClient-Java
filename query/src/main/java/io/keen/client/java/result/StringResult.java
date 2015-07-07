package io.keen.client.java.result;

/**
 * Created by claireyoung on 7/6/15.
 */
public class StringResult extends QueryResult {
    private final String result;

    public StringResult(String result) {
        this.result = result;
    }

    @Override
    public boolean isString() {
        return true;
    }

    @Override
    public String stringValue() {
        return result;
    }
}