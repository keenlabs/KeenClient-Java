package io.keen.client.java;

import java.net.URL;
import java.util.Map;

import io.keen.client.java.exceptions.KeenQueryClientException;

/**
 * This will replace Query, delegating most functionality to the CollectionAnalysis base class,
 * and adding the additional fields useful for some of the single analysis types, like the target
 * property and percentile.
 *
 * @author masojus
 */
public class SingleAnalysis extends CollectionAnalysis {
    // required
    private final QueryType queryType;

    // sometimes optional
    private final String targetPropertyName;

    // required by the Percentile query
    private final Percentile percentile;


    protected SingleAnalysis(Builder builder) {
        super(builder);

        this.queryType = builder.queryType;
        this.targetPropertyName = builder.targetPropertyName;
        this.percentile = builder.percentile;
    }

    @Override
    URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId)
            throws KeenQueryClientException {
        return urlBuilder.getAnalysisUrl(projectId, getAnalysisType());
    }

    @Override
    String getAnalysisType() {
        return queryType.toString();
    }

    @Override
    Map<String, Object> constructRequestArgs() {
        // First get the more general top-level args for this analysis.
        Map<String, Object> singleAnalysisArgs = super.constructRequestArgs();

        if (null != this.targetPropertyName) {
            singleAnalysisArgs.put(KeenQueryConstants.TARGET_PROPERTY, this.targetPropertyName);
        }

        if (null != this.percentile) {
            singleAnalysisArgs.put(KeenQueryConstants.PERCENTILE, this.percentile.asDouble());
        }

        return singleAnalysisArgs;
    }

    @Override
    protected void validateParams() {
        super.validateParams();

        if (null == this.queryType) {
            throw new IllegalArgumentException("A 'queryType' is required.");
        }

        if (QueryType.COUNT == this.queryType && null != this.targetPropertyName) {
            throw new IllegalArgumentException(
                    "Analysis type 'count' should not specify a 'targetPropertyName' parameter.");
        }

        if (QueryType.COUNT != this.queryType
                && (null == this.targetPropertyName || this.targetPropertyName.trim().isEmpty())) {
            throw new IllegalArgumentException(
                    "All but the 'count' analysis type require a target property name.");
        }

        if (QueryType.PERCENTILE == this.queryType ^ null != this.percentile) {
            throw new IllegalArgumentException("Analysis parameter 'percentile' is both " +
                    "required and only allowed for the 'percentile' analysis type.");
        }
    }

    /**
    * This builder class helps configure the required and optional parameters that are appropriate
    * for a SingleAnalysis.
    */
    public static class Builder extends CollectionAnalysis.Builder<Builder> {
        // required
        private QueryType queryType;

        // sometimes optional
        private String targetPropertyName;

        // required by the Percentile query
        private Percentile percentile;


        /**
         * Constructs a SingleAnalysis builder for the given query type.
         *
         * @param queryType The type of query to build.
         */
        public Builder(QueryType queryType) {
            this.queryType = queryType;
        }


        // Configurable properties added by this builder.

        /**
         * Get the target property name.
         *
         * @return The target property name.
         */
        public String getTargetPropertyName() { return this.targetPropertyName; }

        /**
         * Set the target property name. This will replace the existing target property name.
         *
         * @param targetPropertyName The target property name.
         */
        public void setTargetPropertyName(String targetPropertyName) {
            if (null == targetPropertyName || targetPropertyName.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "The 'targetPropertyName' parameter is null or empty.");
            }

            this.targetPropertyName = targetPropertyName;
        }

        /**
         * Set the target property name. This should only happen once.
         *
         * @param targetPropertyName The target property name.
         * @return This instance (for method chaining).
         */
        public Builder withTargetPropertyName(String targetPropertyName) {
            if (null != this.targetPropertyName) {
                throw new IllegalArgumentException("Attempting to set the 'targetPropertyName' " +
                        "parameter for this analysis multiple times, but there can be only one.");
            }

            setTargetPropertyName(targetPropertyName);
            return this;
        }

        /**
         * Get the percentile.
         *
         * @return The percentile to be calculated.
         */
        public Percentile getPercentile() { return this.percentile; }

        /**
         * Set the percentile. This will replace the existing percentile.
         *
         * @param percentile The percentile to be calculated.
         */
        public void setPercentile(Percentile percentile) {
            if (null == percentile) {
                throw new IllegalArgumentException("The 'percentile' parameter cannot be null.");
            }

            this.percentile = percentile;
        }

        /**
         * Set the percentile. This should only happen once.
         *
         * @param percentile The percentile to be calculated.
         * @return This instance (for method chaining).
         */
        public Builder withPercentile(Percentile percentile) {
            if (null != this.percentile) {
                throw new IllegalArgumentException("Attempting to set the 'percentile' " +
                        "parameter for this analysis multiple times, but there can be only one.");
            }

            setPercentile(percentile);
            return this;
        }

        @Override
        protected Builder getThis() {
            return this;
        }

        /**
         * Build the SingleAnalysis after arguments have been set.
         *
         * @return The new SingleAnalysis instance, configured and validated.
         * @throws KeenQueryClientException if validating analysis parameters fails.
         */
        public SingleAnalysis build() {
            SingleAnalysis singleAnalysis = new SingleAnalysis(this);

            // Bail as early as possible if these parameters do not make sense.
            singleAnalysis.validateParams();

            return singleAnalysis;
        }
    }
}
