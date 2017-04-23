package io.keen.client.java.result;

import java.util.Collections;
import java.util.List;

/**
 * ListResult is for if the QueryResult object is of type List.
 *
 * @author claireyoung
 * @since 1.0.0, 07/06/15
 */
public class ListResult extends QueryResult {
    private final List<QueryResult> results;

    /**
     * @param results the result list.
     */
    public ListResult(List<QueryResult> results) {
        this.results = Collections.unmodifiableList(results);
    }

    /**
     * @return  {@code true}
     */
    @Override
    public boolean isListResult() {
        return true;
    }

    /**
     * @return result list.
     */
    @Override
    public List<QueryResult> getListResults() {
        return results;
    }
}