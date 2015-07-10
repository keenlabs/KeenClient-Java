package io.keen.client.java.result;

/**
 * Created by claireyoung on 7/6/15.
 */
public class LongResult extends QueryResult {
    private final long result;

    /**
     * @param result the result.
     */
    public LongResult(long result) {
        this.result = result;
    }

    /**
     * @return  {@code true}
     */
    @Override
    public boolean isLong() {
        return true;
    }

    /**
     * @return long value
     */
    @Override
    public long longValue() {
        return result;
    }
}