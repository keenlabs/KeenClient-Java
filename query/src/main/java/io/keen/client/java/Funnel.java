/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.keen.client.java;

import io.keen.client.java.exceptions.KeenQueryClientException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Object for making funnel analysis requests.
 * @author baumatron
 */
public class Funnel implements KeenRequest {

    private final RequestParameterList steps;
    private final Timeframe timeframe;
    private final String timezone;
    
    public Funnel(Builder builder)
    {
        if (null == builder.steps || builder.steps.isEmpty())
        {
            throw new IllegalArgumentException("Funnel parameter builder.steps must be provided.");
        }
        
        this.steps = new RequestParameterList(builder.steps);
        this.timeframe = builder.timeframe;
        this.timezone = builder.timezone;
    }
    
    /**
     * Get the URL for this request.
     * @param urlBuilder The RequestUrlBuilder instance to use for building the URL.
     * @param projectId The projectId to use for the URL.
     * @return The URL for the request.
     * @throws KeenQueryClientException Thrown if there are errors formatting the URL.
     */
    @Override
    public URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId) throws KeenQueryClientException {
        return urlBuilder.getAnalysisUrl(projectId, KeenQueryConstants.FUNNEL);
    }

    /**
     * Construct the jsonifiable arguments for this request.
     * @return A jsonifiable Map to use for the request body.
     */
    @Override
    public Map<String, Object> constructRequestArgs() {
        HashMap<String, Object> args = new HashMap<String, Object>();
        
        args.put(KeenQueryConstants.STEPS, this.steps.constructParameterRequestArgs());
        
        if (null != this.timezone)
        {
            args.put(KeenQueryConstants.TIMEZONE, this.timezone);
        }
        if (null != this.timeframe)
        {
            args.put(KeenQueryConstants.TIMEFRAME, timeframe.toString());
        }
        
        return args;
    }

    @Override
    public boolean groupedResponseExpected() {
        return false;
    }

    @Override
    public boolean intervalResponseExpected() {
        return false;
    }
    
    /**
     * Builder for creating a Funnel query.
     */
    public static class Builder
    {
        private List<FunnelStep> steps;
        private Timeframe timeframe;
        private String timezone;   
            
        public Builder()
        {
        }
        
        /**
         * Set the list of funnel steps.
         * @param steps The funnel steps.
         */
        public void setSteps(List<FunnelStep> steps)
        {
            this.steps = steps;
        }
        
        /**
         * Set the list of funnel steps.
         * @param steps The funnel steps.
         * @return This Builder instance with the steps added.
         */
        public Builder withSteps(List<FunnelStep> steps)
        {
            this.setSteps(steps);
            return this;
        }
        
        /**
         * Get the list of funnel steps.
         * @return The list of funnel steps.
         */
        public List<FunnelStep> getSteps()
        {
            return this.steps;
        }
        
        /**
         * Add a step to the funnel query.
         * @param step A funnel step.
         * @return The Builder instance with the step added.
         */
        public Builder withStep(FunnelStep step)
        {
            if (null == this.steps)
            {
                this.steps = new ArrayList<FunnelStep>();
            }
            
            this.steps.add(step);
            
            return this;
        }
        
        /**
         * get timeframe
         * @return the timeframe.
         */
        public Timeframe getTimeframe() { return this.timeframe; }

        /**
         * Set timeframe
         * @param timeframe the timeframe.
         */
        public void setTimeframe(Timeframe timeframe) { this.timeframe = timeframe; }

        /**
         * Set timeframe
         * @param timeframe the timeframe.
         * @return This instance (for method chaining).
         */
        public Builder withTimeframe(Timeframe timeframe)
        {
            setTimeframe(timeframe);
            return this;
        }
        
        /**
         * get timezone
         * @return the timezone.
         */
        public String getTimezone() { return this.timezone; }

        /**
         * Set timezone
         * @param timezone the timezone.
         */
        public void setTimezone(String timezone) { this.timezone = timezone; }

        /**
         * Set timezone
         * @param timezone the timezone.
         * @return This instance (for method chaining).
         */
        public Builder withTimezone(String timezone)
        {
            setTimezone(timezone);
            return this;
        }
        
        public Funnel build()
        {
            return new Funnel(this);
        }
    }
    
}
