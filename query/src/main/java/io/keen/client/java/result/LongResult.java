package io.keen.client.java.result;

/**
 * Created by claireyoung on 7/6/15.
 */
public class LongResult extends QueryResult {
    private final long result;

    public LongResult(long result) {
        this.result = result;
    }

    @Override
    public boolean isLong() {
        return true;
    }

    @Override
    public long longValue() {
        return result;
    }
}