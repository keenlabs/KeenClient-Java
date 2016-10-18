/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.keen.client.java;

import java.util.HashMap;
import java.util.List;

/**
 * An object which represents a funnel step.
 * @author baumatron
 */
public class FunnelStep implements RequestParameter {
    
    // Required parameters
    final String eventCollection;
    final String actorProperty;
    final Timeframe timeframe;
    
    // Optional parameters
    final RequestParameterList<Filter> filters;
    final String timezone;
    
    public FunnelStep(
        String eventCollection,
        String actorProperty,
        Timeframe timeframe)
    {
        this(eventCollection, actorProperty, timeframe, null, null);
    }
    
    public FunnelStep(
        String eventCollection,
        String actorProperty,
        Timeframe timeframe,
        List<Filter> filters,
        String timezone)
    {
        if (null == eventCollection || eventCollection.isEmpty())
        {
            throw new IllegalArgumentException("FunnelStep parameter eventCollection must be provided.");
        }
        
        if (null == actorProperty || actorProperty.isEmpty())
        {
            throw new IllegalArgumentException("FunnelStep parameter actorProperty must be provided.");
        }
        
        if (null == timeframe)
        {
            throw new IllegalArgumentException("FunnelStep parameter timeframe must be provided.");
        }
        
        this.eventCollection = eventCollection;
        this.actorProperty = actorProperty;
        this.timeframe = timeframe;
        
        if (null != filters && !filters.isEmpty())
        {
            this.filters = new RequestParameterList(filters);
        }
        else
        {
            this.filters = null;
        }
        
        if (null != timezone && !timezone.isEmpty())
        {
            this.timezone = timezone;
        }
        else
        {
            this.timezone = null;
        }
    }

    /**
     * Constructs the sub-parameters for a funnel step.
     * @return A jsonifiable Map containing the step's request parameters.
     */
    @Override
    public Object constructParameterRequestArgs() {
        HashMap<String, Object> args = new HashMap<String, Object>();
        
        // Add required step parameters
        args.put(KeenQueryConstants.EVENT_COLLECTION, eventCollection);
        args.put(KeenQueryConstants.ACTOR_PROPERTY, actorProperty);
        args.put(KeenQueryConstants.TIMEFRAME, timeframe.toString());
        
        // Add optional parameters if they've been specified.
        if (null != this.filters)
        {
            args.put(
                KeenQueryConstants.FILTERS,
                this.filters.constructParameterRequestArgs()
            );
        }
        
        if (null != timezone)
        {
            args.put(KeenQueryConstants.TIMEZONE, timezone);
        }
        
        return args;
    }
    
}
