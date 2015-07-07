package io.keen.client.java.result;

/**
 * Created by claireyoung on 7/6/15.
 */
public class StringResult extends QueryResult {
    private final String result;

    /**
     * @param result the result.
     */
    public StringResult(String result) {
        this.result = result;
    }

    /**
     * @return  {@code true}
     */
    @Override
    public boolean isString() {
        return true;
    }

    /**
     * @return String value.
     */
    @Override
    public String stringValue() {
        return result;
    }
}