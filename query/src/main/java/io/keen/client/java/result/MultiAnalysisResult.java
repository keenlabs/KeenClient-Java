package io.keen.client.java.result;

import java.util.Collections;
import java.util.Map;

/**
 * Represents a mapping of sub-analysis labels, as specified in a MultiAnalysis, to QueryResults.
 *
 * @author masojus
 */
public class MultiAnalysisResult extends QueryResult {
    private final Map<String, QueryResult> analysesResults;

    /**
     * Constructs a MultiAnalysisResult to hold the given mapping of labels to results.
     *
     * @param analysesResults The map of sub-analysis labels to results.
     */
    public MultiAnalysisResult(Map<String, QueryResult> analysesResults) {
        this.analysesResults = Collections.unmodifiableMap(analysesResults);
    }

    /**
     * Provides the result for a sub-analysis.
     *
     * @param subAnalysisLabel The label that was assigned to this sub-analysis in the
     *                         MultiAnalysis request.
     * @return The result of the given sub-anlysis.
     */
    public QueryResult getResultFor(String subAnalysisLabel) {
        if (!this.analysesResults.containsKey(subAnalysisLabel)) {
            throw new IllegalArgumentException("No results for a sub-analysis with that label.");
        }

        return this.analysesResults.get(subAnalysisLabel);
    }

    /**
     * Provides the mapping of all results.
     *
     * @return An unmodifiable mapping of all sub-analysis labels to results.
     */
    public Map<String, QueryResult> getAllResults() {
        return this.analysesResults;
    }
}
