package io.keen.client.java.result;

import java.util.Collections;
import java.util.List;

/**
 * Created by claireyoung on 7/6/15.
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