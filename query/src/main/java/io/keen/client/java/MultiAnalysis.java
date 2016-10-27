package io.keen.client.java;

import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import io.keen.client.java.exceptions.KeenQueryClientException;

/**
 * Represents a Multi-Analysis. This type of analysis targets an event collection and specifies
 * a set of sub-analyses described in the
 * <a href="https://keen.io/docs/api/#multi-analysis">API Docs</a>.
 *
 * @author masojus
 */
public class MultiAnalysis extends CollectionAnalysis {
    // required
    private final Collection<SubAnalysis> analyses;


    protected MultiAnalysis(Builder builder) {
        super(builder);

        this.analyses = builder.subAnalyses;
    }

    @Override
    URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId)
            throws KeenQueryClientException {
        return urlBuilder.getAnalysisUrl(projectId, KeenQueryConstants.MULTI_ANALYSIS);
    }

    @Override
    Map<String, Object> constructRequestArgs() {
        // First get the more general top-level args for this analysis.
        Map<String, Object> multiAnalysisArgs = super.constructRequestArgs();

        // validateParams() makes sure this.analyses is properly configured.
        Map<String, Map<String, Object>> analysesArgs = new HashMap<String, Map<String, Object>>();

        // Add each of the sub-analyses as "label": { ...sub-analysis args... }
        for (SubAnalysis subAnalysis : this.analyses) {
            Map<String, Object> subAnalysisArgs = subAnalysis.constructParameterRequestArgs();

            analysesArgs.put(subAnalysis.getLabel(), subAnalysisArgs);
        }

        // Add the top-level "analyses" key
        multiAnalysisArgs.put(KeenQueryConstants.ANALYSES, analysesArgs);

        return multiAnalysisArgs;
    }

    @Override
    protected void validateParams() {
        super.validateParams();

        if (null == this.analyses || this.analyses.isEmpty()) {
            throw new IllegalArgumentException("MultiAnalysis requires that 'analyses' be set.");
        }
    }

    /**
     * This builder class helps configure the required and optional parameters that are appropriate
     * for a MultiAnalysis.
     */
    public static class Builder extends CollectionAnalysis.Builder<Builder> {
        // required
        private Collection<SubAnalysis> subAnalyses;


        /**
         * Get the collection of sub-analyses for this multi-analysis.
         *
         * @return The sub-analyses.
         */
        public Collection<SubAnalysis> getSubAnalyses() { return this.subAnalyses; }


        // Configurable properties added by this builder.

        /**
         * Set the collection of sub-analyses. Existing sub-analyses will be discarded.
         *
         * @param subAnalyses The new analysis arguments to replace the discarded ones, if any.
         */
        public void setSubAnalyses(Collection<? extends SubAnalysis> subAnalyses) {
            this.subAnalyses = null;

            // Client code may just be clearing all the analyses.
            if (null != subAnalyses) {
                withSubAnalyses(subAnalyses);
            }
        }

        /**
         * Adds a collection of sub-analyses.
         *
         * @param subAnalyses The new analysis arguments to add to the existing analyses.
         * @return This instance (for method chaining).
         */
        public Builder withSubAnalyses(Collection<? extends SubAnalysis> subAnalyses) {
            if (null == subAnalyses) {
                throw new IllegalArgumentException("The 'subAnalyses' parameter cannot be null.");
            }

            // Add each sub-analysis to the list, appending to anything that already exists.
            for (SubAnalysis subAnalysis : subAnalyses) {
                addSubAnalysis(subAnalysis);
            }

            return this;
        }

        /**
         * Adds a sub-analysis to the existing analyses.
         *
         * @param subAnalysis The sub-analysis to add to the existing analyses.
         * @return This instance (for method chaining).
         */
        public Builder withSubAnalysis(SubAnalysis subAnalysis) {
            addSubAnalysis(subAnalysis);
            return this;
        }

        /**
         * Adds a group by property to the list of existing group by properties.
         *
         * @param subAnalysis TThe sub-analysis to add to the existing analyses.
         */
        public void addSubAnalysis(SubAnalysis subAnalysis) {
            if (null == subAnalysis) {
                throw new IllegalArgumentException("The 'subAnalysis' parameter cannot be null.");
            }

            if (null == this.subAnalyses) {
                // No point in specifying one twice. Though obviously for now, since SubAnalysis
                // has no custom equality, this won't prevent the user from specifying two different
                // instances representing the exact same analysis. That said, even if the JSON
                // implementation allowed two sub-analyses with the same key name, the back end will
                // just return one, and that'd be pointless anyway. Ideally this builder could warn
                // the client dev in validateParams() that one of their sub-analyses will be
                // ignored, which likely highlights an error in client code.
                this.subAnalyses = new HashSet<SubAnalysis>();
            }

            this.subAnalyses.add(subAnalysis);
        }

        @Override
        protected Builder getThis() {
            return this;
        }

        /**
         * Build the MultiAnalysis after arguments have been set.
         *
         * @return The new MultiAnalysis instance, configured and validated.
         * @throws KeenQueryClientException if validating analysis parameters fails.
         */
        public MultiAnalysis build() throws KeenQueryClientException {
            MultiAnalysis multiAnalysis = new MultiAnalysis(this);

            // Bail as early as possible if these parameters do not make sense.
            multiAnalysis.validateParams();

            return multiAnalysis;
        }
    }
}
