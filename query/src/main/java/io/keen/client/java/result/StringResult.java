package io.keen.client.java.result;

/**
 * StringResult is for if the QueryResult object is of type String.
 *
 * @author claireyoung
 * @since 1.0.0, 07/06/15
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

    @Override
    public String toString() {
        return "StringResult{" +
                "result='" + result + '\'' +
                '}';
    }
}
