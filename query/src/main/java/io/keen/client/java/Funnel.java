package io.keen.client.java;

import io.keen.client.java.exceptions.KeenQueryClientException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Object for making funnel analysis requests.
 * 
 * @author baumatron
 */
public class Funnel extends KeenQueryRequest {

    // Required parameters
    private final RequestParameterCollection steps;
    
    // Optional parameters
    // Timeframe must be specified either on the Funnel, or for each
    // FunnelStep. If specified for the Funnel, it can be overriden
    // in the FunnelSteps
    private final Timeframe timeframe;
    
    /**
     * Private constructor for a Funnel, called by Funnel.Builder. To create a
     * funnel, use an instance of the Funnel.Builder class.
     * 
     * @param builder The builder to use for building the Funnel object.
     */
    private Funnel(final Builder builder) {
        
        if (null == builder.steps || builder.steps.isEmpty()) {
            throw new IllegalArgumentException("Funnel parameter builder.steps must be provided.");
        }
        
        if (null != builder.timeframe) {
            this.timeframe = builder.timeframe;
        }
        else {
            this.timeframe = null;
            // If no timeframe has been specified for the funnel, 
            // each step needs to provide one.
            for (FunnelStep step : builder.steps) {
                if (null == step.getTimeframe()) {
                    throw new IllegalArgumentException(
                        "A funnel step is missing a timeframe but no root "
                      + "timeframe was provided for the funnel request.");
                }
            }
        }
        
        // Validate step properties that cannot be true for the first funnel step
        FunnelStep firstStep = builder.steps.get(0);
        
        if (null != firstStep.getInverted() &&
            true == firstStep.getInverted())
        {
            throw new IllegalArgumentException(
                "First step in funnel cannot have special parameter 'inverted' set to true.");
        }
        
        if (null != firstStep.getOptional() &&
            true == firstStep.getOptional())
        {
            throw new IllegalArgumentException(
                "First step in funnel cannot have special parameter 'optional' set to true.");
        }
        
        this.steps = new RequestParameterCollection(builder.steps);
    }
    
    /**
     * Get the URL for this request.
     * 
     * @param urlBuilder The RequestUrlBuilder instance to use for building the URL.
     * @param projectId The projectId to use for the URL.
     * @return The URL for the request.
     * @throws KeenQueryClientException Thrown if there are errors formatting the URL.
     */
    @Override
    URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId) throws KeenQueryClientException {
        return urlBuilder.getAnalysisUrl(projectId, KeenQueryConstants.FUNNEL);
    }

    /**
     * Construct the jsonifiable arguments for this request.
     * 
     * @return A jsonifiable Map to use for the request body.
     */
    @Override
    Map<String, Object> constructRequestArgs() {
        
        Map<String, Object> args = new HashMap<String, Object>();
        
        args.put(KeenQueryConstants.STEPS, this.steps.constructParameterRequestArgs());
        
        if (null != this.timeframe) {
            args.putAll(timeframe.constructTimeframeArgs());
        }
        
        return args;
    }

    @Override
    boolean groupedResponseExpected() { return false; }

    @Override
    boolean intervalResponseExpected() { return false; }
    
    /**
     * Builder for creating a Funnel query.
     */
    public static class Builder {
        
        private List<FunnelStep> steps;
        private Timeframe timeframe;
        
        /**
         * Set the list of funnel steps.
         * 
         * @param steps The funnel steps.
         */
        public void setSteps(List<FunnelStep> steps) { this.steps = steps; }
        
        /**
         * Set the list of funnel steps.
         * 
         * @param steps The funnel steps.
         * @return This Builder instance with the steps added.
         */
        public Builder withSteps(List<FunnelStep> steps) {
            
            // Add each step to the list of steps, appending to anything
            // that already exists.
            for (FunnelStep step : steps) {
                this.withStep(step);
            }
            
            return this;
        }
        
        /**
         * Get the list of funnel steps.
         * 
         * @return The list of funnel steps.
         */
        public List<FunnelStep> getSteps() { return this.steps; }
        
        /**
         * Add a step to the funnel query.
         * 
         * @param step A funnel step to add.
         * @return The Builder instance with the step added.
         */
        public Builder withStep(FunnelStep step) {
            this.addStep(step);
            return this;
        }
        
        /**
         * Add a step to the funnel query.
         *
         * @param step A funnel step to add.
         */
        public void addStep(FunnelStep step) {
            
            if (null == this.steps) {
                this.steps = new ArrayList<FunnelStep>();
            }
            
            this.steps.add(step);
        }
        
        /**
         * get timeframe
         * 
         * @return the timeframe.
         */
        public Timeframe getTimeframe() { return this.timeframe; }

        /**
         * Set timeframe
         * 
         * @param timeframe the timeframe.
         */
        public void setTimeframe(Timeframe timeframe) { this.timeframe = timeframe; }

        /**
         * Set timeframe
         * 
         * @param timeframe the timeframe.
         * @return This instance (for method chaining).
         */
        public Builder withTimeframe(Timeframe timeframe) {
            
            if (null != this.timeframe) {
                throw new IllegalStateException("'withTimeframe' called, but a Timeframe instance has already been set.");
            }
            
            setTimeframe(timeframe);
            return this;
        }
        
        /**
         * Creates a Funnel instance using parameters passed to the Builder instance.
         *
         * @return A Funnel query instance with the properties provided to the Builder instance.
         */
        public Funnel build() { return new Funnel(this); }
    }
    
}
