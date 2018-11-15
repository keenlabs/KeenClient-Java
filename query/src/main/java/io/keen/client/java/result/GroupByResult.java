package io.keen.client.java.result;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Collections;
import java.util.Map;

/**
 * <p>GroupByResult is for if Group By properties were specified in the query.
 * If so, this object contains a Map that consist of </p>
 * <ul>
 * <li>Group, which contains the unique property/values</li>
 * <li>QueryResult, which is the result of the query as grouped by Group</li>
 * </ul>
 *
 * @author claireyoung
 * @since 1.0.0, 07/06/15
 */
public class GroupByResult extends QueryResult {
    private final Map<Group, QueryResult> results;

    /**
     * @param results the result map of Group to QueryResult.
     */
    public GroupByResult(Map<Group, QueryResult> results) {
        this.results = Collections.unmodifiableMap(results);
    }

    /**
     * @return  {@code true}
     */
    @Override
    public boolean isGroupResult() {
        return true;
    }

    /**
     * @return map of Group to QueryResult objects
     */
    @Override
    public Map<Group, QueryResult> getGroupResults() {
        return results;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
