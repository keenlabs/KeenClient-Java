package io.keen.client.java;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * An object which represents a funnel step.
 * 
 * @author baumatron
 */
public class FunnelStep extends RequestParameter {
        
    // Required parameters
    private final String collectionName;
    private final String actorPropertyName;
    private final Timeframe timeframe;
    
    // Optional parameters
    private final RequestParameterCollection<Filter> filters;
    
    public FunnelStep(
        final String collectionName,
        final String actorPropertyName) {
        
        // timeframe can be unspecified for a funnel step, but only if it
        // has been specified at for the root request
        this(collectionName, actorPropertyName, null, null);
    }
        
    public FunnelStep(
        final String collectionName,
        final String actorPropertyName,
        Timeframe timeframe) {
        
        this(collectionName, actorPropertyName, timeframe, null);
    }
    
    public FunnelStep(
        final String collectionName,
        final String actorPropertyName,
        final Timeframe timeframe,
        final Collection<Filter> filters) {
        
        if (null == collectionName || collectionName.trim().isEmpty()) {
            throw new IllegalArgumentException("FunnelStep parameter collectionName must be provided.");
        }
        
        if (null == actorPropertyName || actorPropertyName.trim().isEmpty()) {
            throw new IllegalArgumentException("FunnelStep parameter actorPropertyName must be provided.");
        }
        
        this.collectionName = collectionName;
        this.actorPropertyName = actorPropertyName;
        
        // Timeframe may be null
        this.timeframe = timeframe;
        
        if (null != filters && !filters.isEmpty()) {
            this.filters = new RequestParameterCollection(filters);
        }
        else {
            this.filters = null;
        }
    }

    /**
     * Constructs the sub-parameters for a funnel step.
     * 
     * @return A jsonifiable Map containing the step's request parameters.
     */
    @Override
    Object constructParameterRequestArgs() {
        
        Map<String, Object> args = new HashMap<String, Object>();
        
        // Add required step parameters
        args.put(KeenQueryConstants.EVENT_COLLECTION, collectionName);
        args.put(KeenQueryConstants.ACTOR_PROPERTY, actorPropertyName);
        
        // timeframe is only required if not specified for the funnel itself,
        // so it may not be set.
        if (null != this.timeframe) {
            args.putAll(timeframe.constructTimeframeArgs());
        }

        // Add optional parameters if they've been specified.
        if (null != this.filters) {
            args.put(
                KeenQueryConstants.FILTERS,
                this.filters.constructParameterRequestArgs()
            );
        }
        
        return args;
    }
    
    /**
     * Package visible accessor for validating Funnel timeframe arguments.
     * 
     * @return 
     */
    Timeframe getTimeframe() { return this.timeframe; }
}
