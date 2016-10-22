/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.keen.client.java.result;

/**
 *
 * @author baumatron
 */
public class FunnelResult extends QueryResult {
    
    /**
     * The results of the funnel query
     */
    private final ListResult funnelResult;
    
    /**
     * If provided, the list of actor property values
     * for funnel steps that have specified the 'with_actors'
     * special property.
     */
    private final ListResult actorsResult;
    
    /**
     * Constructs a FunnelResult given the funnel query result and optionally
     * the actor field value results.
     * 
     * @param result        The ListResult for the funnel query.
     * @param actorsResult  Optionally, the actor property value result.
     */
    public FunnelResult(ListResult result, ListResult actorsResult) {
        
        if (null == result) {
            throw new IllegalArgumentException("No result value provided");
        }
        
        this.funnelResult = result;
        this.actorsResult = actorsResult;
    }
    
    /**
     * 
     * @return The ListResult for the actors property in the result response.
     */
    public ListResult getActorResult() { return this.actorsResult; }
    
    /**
     * 
     * @return The ListResult containing the results of the funnel query.
     */
    public ListResult getFunnelResult() { return this.funnelResult; }
}
