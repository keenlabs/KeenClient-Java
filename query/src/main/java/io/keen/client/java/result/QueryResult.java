package io.keen.client.java.result;

import java.util.List;
import java.util.Map;

import io.keen.client.java.AbsoluteTimeframe;

/**
 * This abstract class represents the object returned by a Keen Query.
 * By default, all methods with boolean return values are set to return false,
 * and all getter methods are set to throw IllegalStateException.
 *
 * It is the responsibility of subclasses to return true and get the
 * appropriate object.
 *
 * @author claireyoung
 * @since 1.0.0, 07/06/15
 */
public abstract class QueryResult {

    /**
     * @return {@code false}
     */
    public boolean isDouble() {
        return false;
    }

    /**
     * @return {@code false}
     */
    public boolean isLong() {
        return false;
    }

    /**
     * @return {@code false}
     */
    public boolean isString() {
        return false;
    }

    /**
     * @return {@code false}
     */
    public boolean isListResult() {
        return false;
    }

    /**
     * @return {@code false}
     */
    public boolean isIntervalResult() {
        return false;
    }

    /**
     * @return {@code false}
     */
    public boolean isGroupResult() { return false; }

    /**
     * @return doubleValue, which is IllegalStateException in abstract class.
     */
    public double doubleValue() {
        throw new IllegalStateException();
    }

    /**
     * @return longValue, which is IllegalStateException in abstract class.
     */
    public long longValue() {
        throw new IllegalStateException();
    }

    /**
     * @return stringValue, which is IllegalStateException in abstract class.
     */
    public String stringValue() {
        throw new IllegalStateException();
    }

    /**
     * @return list results, which is IllegalStateException in abstract class.
     */
    public List<QueryResult> getListResults() {
        throw new IllegalStateException();
    }

    /**
     * @return map of AbsoluteTimeframe to QueryResult's, which is IllegalStateException in abstract class.
     */
    public Map<AbsoluteTimeframe, QueryResult> getIntervalResults() { throw new IllegalStateException(); }

    /**
     * @return map of Group to QueryResult's, which is IllegalStateException in abstract class.
     */
    public Map<Group, QueryResult> getGroupResults() { throw new IllegalStateException(); }

}
