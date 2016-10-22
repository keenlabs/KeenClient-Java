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
    /**
     * The name of the event collection.
     */
    private final String collectionName;
    
    /**
     * The name of the actor property for this funnel step.
     */
    private final String actorPropertyName;
    
    /**
     * The timeframe used to qualify the events used for this funnel step.
     * If not specified here for the funnel step, must be specified on the funnel
     * itself.
     */
    private final Timeframe timeframe;
    
    // Optional parameters
    /**
     * An optional list of filters used to qualify the events used in 
     * this funnel step.
     */
    private final RequestParameterCollection<Filter> filters;
    
    // Special parameters
    /**
     * If true, inverted means events matching this step will be excluded from
     * the funnel results.
     * Cannot be set if this is the first step in a funnel query.
     */
    private final Boolean inverted;
    
    /**
     * If optional is set, the funnel will ignore the effects of this step
     * on subsequent steps.
     */
    private final Boolean optional;
    
    /**
     * If set, the query result will include a list of actor property values
     * for this step.
     */
    private final Boolean withActors;
    
    /**
     * FunnelStep constructor including all required, optional, and special parameters.
     * Any parameter that is optional may be set to null.
     *
     * @param collectionName    The name of the event collection for the funnel step.
     * @param actorPropertyName The name of the actor property for the funnel step.
     * @param timeframe         The timeframe for the set of events to include,
     *                          optional if a timeframe is set for the entire funnel.
     * @param filters           Optionally, any filters to use for the set of events
     *                          to include in this step.
     * @param inverted          When set to true will exclude events matching
     *                          this step from the funnel. Optional and cannot
     *                          be set to true for the first funnel step
     * @param optional          When set to true the funnel will not apply the
     *                          effects of this step to subsequent steps.
     *                          Optional and cannot be set to true for the first
     *                          funnel step. 
     * @param withActors        Optional, and when set to true will include values
     *                          for the actor property for this step in the results
     *                          of the query.
     */
    public FunnelStep(
        final String collectionName,
        final String actorPropertyName,
        final Timeframe timeframe,
        final Collection<Filter> filters,
        final Boolean inverted,
        final Boolean optional,
        final Boolean withActors) {
        
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
        
        // Special properties
        this.inverted = inverted;
        this.optional = optional;
        this.withActors = withActors;
    }
    
    /**
     * Constructor with only most basic required parameters.
     * Does not include timeframe, meaning timeframe must be specified for the
     * Funnel itself.
     *
     * @param collectionName    The name of the event collection for the funnel step.
     * @param actorPropertyName The name of the actor property for the funnel step.
     */
    public FunnelStep(
        final String collectionName,
        final String actorPropertyName) {
        
        // timeframe can be unspecified for a funnel step, but only if it
        // has been specified at for the root request
        this(collectionName, actorPropertyName, null);
    }
    
    /**
     * Constructor with required parameters.
     * Includes timeframe, which is required if not specified on the Funnel
     * itself
     * 
     * @param collectionName    The name of the event collection for the funnel step.
     * @param actorPropertyName The name of the actor property for the funnel step.
     * @param timeframe         The timeframe for the set of events to include,
     *                          optional if a timeframe is set for the entire funnel.
     */
    public FunnelStep(
        final String collectionName,
        final String actorPropertyName,
        Timeframe timeframe) {
        
        this(collectionName, actorPropertyName, timeframe, null, null, null, null);
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
        
        // Add special parameters if they are set
        if (null != this.inverted) {
            args.put(
                KeenQueryConstants.INVERTED,
                this.inverted
            );
        }
        
        if (null != this.optional) {
            args.put(
                KeenQueryConstants.OPTIONAL,
                this.optional
            );
        }
        
        if (null != this.withActors) {
            args.put(
                KeenQueryConstants.WITH_ACTORS,
                this.withActors
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
    
    /**
     * Get the value of the inverted special property. Will return null
     * if the property is not specified.
     * 
     * @return The value of 'inverted'
     */
    Boolean getInverted() { return this.inverted; }
    
    /**
     * Get the value of the optional special property. Will return null
     * if the property is not specified.
     * 
     * @return The value of 'optional'
     */
    Boolean getOptional() { return this.optional; }
}
