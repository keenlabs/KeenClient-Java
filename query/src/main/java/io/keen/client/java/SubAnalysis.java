package io.keen.client.java;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.HashMap;
import java.util.Map;

/**
 * Each instance of this class represents one of potentially several individual analyses comprising
 * a request to perform a <a href="https://keen.io/docs/api/#multi-analysis">Multi-Analysis</a>.
 *
 * @author masojus
 */
public class SubAnalysis extends RequestParameter<Map<String, Object>> {
    private final String label;
    private final QueryType analysisType;
    private final String targetPropertyName;
    private final Percentile percentile;

    /**
     * Constructs a SubAnalysis with only label and analysis type provided. This should really
     * only apply to a sub-analysis of type 'count' since all others require a target property name
     * to be specified.
     *
     * @param label The label for this sub-analysis.
     * @param analysisType The type of sub-analysis.
     */
    public SubAnalysis(String label, QueryType analysisType) {
        this(label, analysisType, null);
    }

    /**
     * Constructs a SubAnalysis with only target property name in addition to label and analysis
     * type. This covers most types of sub-analysis except 'count' and 'percentile' types.
     *
     * @param label The label for this sub-analysis.
     * @param analysisType The type of sub-analysis.
     * @param targetPropertyName The collection property on which to run this sub-analysis.
     */
    public SubAnalysis(String label, QueryType analysisType, String targetPropertyName) {
        this(label, analysisType, targetPropertyName, null);
    }

    /**
     *
     * @param label The label for this sub-analysis.
     * @param analysisType The type of sub-analysis.
     * @param targetPropertyName The collection property on which to run this sub-analysis.
     * @param percentile The percentile to be calculated.
     */
    public SubAnalysis(String label,
                       QueryType analysisType,
                       String targetPropertyName,
                       Percentile percentile) {
        if (null == label || label.trim().isEmpty()) {
            throw new IllegalArgumentException("SubAnalysis parameter 'label' is required.");
        }

        if (null == analysisType) {
            throw new IllegalArgumentException("SubAnalysis parameter 'analysisType' is required.");
        }

        if (QueryType.COUNT != analysisType
                && (null == targetPropertyName || targetPropertyName.trim().isEmpty())) {
            throw new IllegalArgumentException(
                    "All SubAnalysis types but 'count' require a target property name.");
        }

        if (QueryType.COUNT == analysisType && null != targetPropertyName) {
            throw new IllegalArgumentException("SubAnalysis of analysis type 'count' should not " +
                "specify a 'targetPropertyName' parameter.");
        }

        if (QueryType.PERCENTILE == analysisType ^ null != percentile) {
            throw new IllegalArgumentException("SubAnalysis parameter 'percentile' is both " +
                    "required and only allowed for the 'percentile' analysis type.");
        }

        this.label = label;
        this.analysisType = analysisType;
        this.targetPropertyName = targetPropertyName;
        this.percentile = percentile;
    }

    /**
     * Get this sub-analysis' label.
     *
     * @return The label for this sub-analysis.
     */
    public String getLabel() { return this.label; }

    /**
     * Constructs request sub-parameters for this SubAnalysis.
     *
     * {@inheritDoc}
     *
     * @return A jsonifiable object that should be the value for a key with the name of the label
     *      of this SubAnalysis instance.
     */
    @Override
    Map<String, Object> constructParameterRequestArgs() {
        // The label needs to be the key and this object is the value. Unfortunately that means
        // whatever parent object calls this needs to know to use this label as the key.
        // We could fix this by changing RequestParameter's interface to take the outer Map<> as a
        // parameter, and then each RequestParameter would know whether to add a Map<> or a
        // Collection<> or just add a (K, V).
        Map<String, Object> subAnalysisInfo = new HashMap<String, Object>(4);

        subAnalysisInfo.put(KeenQueryConstants.ANALYSIS_TYPE, this.analysisType.toString());

        if (QueryType.COUNT != this.analysisType) {
            subAnalysisInfo.put(KeenQueryConstants.TARGET_PROPERTY, this.targetPropertyName);
        }

        if (QueryType.PERCENTILE == analysisType) {
            // Put the raw Double in here for JSON serialization for now
            subAnalysisInfo.put(KeenQueryConstants.PERCENTILE, this.percentile.asDouble());
        }

        return subAnalysisInfo;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
