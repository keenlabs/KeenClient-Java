package io.keen.client.java;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.keen.client.java.exceptions.KeenQueryClientException;

/**
 * Query represents all the details of the query to be run, including required
 * and optional parameters.
 *
 * Created by claireyoung on 5/18/15.
 * @author claireyoung, baumatron, masojus
 * @since 1.0.0
 */
public class Query extends KeenQueryRequest {
    // required
    private final QueryType queryType;
    private final String eventCollection;
    private final Timeframe timeframe;

    // sometimes optional
    private final String targetProperty;

    // optional
    private final RequestParameterCollection<Filter> filters;
    private final String interval;    // requires timeframe to be set
    private final List<String> groupBy;
    private final Integer maxAge; // integer greater than 30 seconds: https://keen.io/docs/data-analysis/caching/

    // required by the Percentile query
    private final Double percentile;  // 0-100 with two decimal places of precision for example, 99.99

    /**
     * Constructs the map to pass to the JSON handler, so that the proper required
     * and optional Query arguments can be sent out to the server.
     *
     * @return The JSON object map.
     */
    @Override
    Map<String, Object> constructRequestArgs() {
        Map<String, Object> queryArgs = new HashMap<String, Object>();

        if (eventCollection != null) {
            queryArgs.put(KeenQueryConstants.EVENT_COLLECTION, eventCollection);
        }

        if (interval != null) {
            queryArgs.put(KeenQueryConstants.INTERVAL, interval);
        }

        if (groupBy != null) {
            queryArgs.put(KeenQueryConstants.GROUP_BY, groupBy);
        }

        if (maxAge != null) {
            queryArgs.put(KeenQueryConstants.MAX_AGE, maxAge);
        }

        if (targetProperty != null) {
            queryArgs.put(KeenQueryConstants.TARGET_PROPERTY, targetProperty);
        }

        if (percentile != null) {
            queryArgs.put(KeenQueryConstants.PERCENTILE, percentile);
        }

        if (filters != null) {
            queryArgs.put(KeenQueryConstants.FILTERS, filters.constructParameterRequestArgs());
        }

        if (timeframe != null) {
            queryArgs.putAll(timeframe.constructTimeframeArgs());
        }

        return queryArgs;
    }

    /**
     * @return the query type
     */
    public QueryType getQueryType() { return queryType; }

    /**
     * @return whether this query has a GroupBy specified.
     */
    public boolean hasGroupBy() { return groupBy != null; }

    /**
     * @return whether this query has an Interval specified.
     */
    public boolean hasInterval() { return interval != null; }

    /**
     * Verifies whether the parameters are valid, based on the input query name.
     *
     * @return       whether the parameters are valid.
     */
    public boolean areParamsValid() {
        if (queryType == QueryType.COUNT) {
            if (eventCollection == null || eventCollection.isEmpty()) {
                return false;
            }
        }

        if (queryType == QueryType.COUNT_UNIQUE
                || queryType == QueryType.MINIMUM || queryType == QueryType.MAXIMUM
                || queryType == QueryType.AVERAGE || queryType == QueryType.MEDIAN
                || queryType == QueryType.PERCENTILE || queryType == QueryType.SUM
                || queryType == QueryType.SELECT_UNIQUE) {

            if (eventCollection == null || eventCollection.isEmpty() || targetProperty == null || targetProperty.isEmpty()) {
                return false;
            }
        }

        if (queryType == QueryType.PERCENTILE) {
            if (percentile == null) {
                return false;
            }
        }

        return true;
    }

    /**
     * Constructs a Keen Query using a Builder.
     *
     * @param builder The builder from which to retrieve this client's interfaces and settings.
     */
    protected Query(Builder builder) {
        this.eventCollection = builder.eventCollection;
        this.targetProperty = builder.targetProperty;
        this.interval = builder.interval;
        this.groupBy = builder.groupBy;
        this.maxAge = builder.maxAge;
        this.percentile = builder.percentile;
        this.queryType = builder.queryType;
        this.timeframe = builder.timeframe;

        if (null != builder.filters && !builder.filters.isEmpty()) {
            this.filters = new RequestParameterCollection<Filter>(builder.filters);
        } else {
            this.filters = null;
        }
    }

    @Override
    URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId) throws KeenQueryClientException {
        return urlBuilder.getAnalysisUrl(
            projectId,
            this.queryType.toString());
    }

    @Override
    boolean groupedResponseExpected() {
        return this.hasGroupBy();
    }

    @Override
    boolean intervalResponseExpected() {
        return this.hasInterval();
    }

    @Override
    Collection<String> getGroupByParams() {
        return this.groupBy;
    }

    /**
     * Builder to construct a query with required and optional arguments.
     * Note that the only required argument in this builder is QueryType, although
     * individual queries may require additional arguments.
     */
    public static class Builder {
        // required
        private QueryType queryType;
        private String eventCollection;
        private Timeframe timeframe;

        // sometimes optional
        private String targetProperty;

        // required by the Percentile query
        private Double percentile;    // 0-100 with two decimal places of precision for example, 99.99

        // optional
        private Collection<Filter> filters;
        private String interval;
        private List<String> groupBy;
        private Integer maxAge;

        public Builder(QueryType queryType) {
            this.queryType = queryType;
        }

        /**
         * Get filters
         *
         * @return a collection of filters.
         */
        public Collection<Filter> getFilters() { return filters; }

        /**
         * Set the list of filters. Existing filters will be discarded.
         *
         * @param filters the filter arguments.
         */
        public void setFilters(Collection<? extends Filter> filters) {
            this.filters = null;

            // Client code may just be clearing all the filters.
            if (null != filters) {
                withFilters(filters);  // Shallow copy the list of steps
            }
        }

        /**
         * Adds the given filters as optional parameters to the query.
         *
         * @param filters the filter arguments.
         * @return This instance (for method chaining).
         */
        public Builder withFilters(Collection<? extends Filter> filters) {
            // Add each filter to the list of filters, appending to anything
            // that already exists.
            for (Filter filter : filters) {
                addFilter(filter);
            }

            return this;
        }

        /**
         * Adds a filter as an optional parameter to the query.
         * Refer to API documentation: https://keen.io/docs/data-analysis/filters/
         *
         * @param propertyName     The name of the property.
         * @param operator          The operator (eg., gt, lt, exists, contains)
         * @param propertyValue       The property value. Refer to API documentation for info.
         *                            This can be a string, number, boolean, or geo-coordinates
         *                            and are based on what the operator is.
         * @return This instance (for method chaining).
         */
        public Builder withFilter(String propertyName, FilterOperator operator, Object propertyValue) {
            addFilter(propertyName, operator, propertyValue);
            return this;
        }
        
        /**
         * Adds a filter as an optional parameter to the query.
         * Refer to API documentation: https://keen.io/docs/data-analysis/filters/
         * 
         * @param propertyName  The name of the property.
         * @param operator      The operator (eg., gt, lt, exists, contains)
         * @param propertyValue The property value. Refer to API documentation for info.
         *                      This can be a string, number, boolean, or geo-coordinates
         *                      and are based on what the operator is.
         */
        public void addFilter(String propertyName, FilterOperator operator, Object propertyValue) {
            Filter filter = new Filter(propertyName, operator, propertyValue);

            addFilter(filter);
        }

        /**
         * Adds a filter as an optional parameter to the query.
         * Refer to API documentation: https://keen.io/docs/data-analysis/filters/
         *
         * @param filter the filter to add to the list of filters.
         */
        public void addFilter(Filter filter) {
            if (filters == null) {
                filters = new LinkedList<Filter>();
            }

            filters.add(filter);
        }

        /**
         * Get event collection
         *
         * @return the event collection.
         */
        public String getEventCollection() { return eventCollection; }

        /**
         * Set event collection
         *
         * @param eventCollection the event collection.
         */
        public void setEventCollection(String eventCollection) { this.eventCollection = eventCollection; }

        /**
         * Set event collection
         *
         * @param eventCollection the event collection.
         * @return This instance (for method chaining).
         */
        public Builder withEventCollection(String eventCollection) {
            setEventCollection(eventCollection);
            return this;
        }

        /**
         * Get target property
         *
         * @return the target property.
         */
        public String getTargetProperty() { return targetProperty; }

        /**
         * Set target property
         *
         * @param targetProperty the target property.
         */
        public void setTargetProperty(String targetProperty) { this.targetProperty = targetProperty; }

        /**
         * Set target property
         *
         * @param targetProperty the target property.
         * @return This instance (for method chaining).
         */
        public Builder withTargetProperty(String targetProperty) {
            setTargetProperty(targetProperty);
            return this;
        }

        /**
         * Get Interval
         *
         * @return the interval.
         */
        public String getInterval() { return interval; }

        /**
         * Set interval
         *
         * @param interval the interval.
         */
        public void setInterval(String interval) { this.interval = interval; }

        /**
         * Set interval
         *
         * @param interval the interval.
         * @return This instance (for method chaining).
         */
        public Builder withInterval(String interval) {
            setInterval(interval);
            return this;
        }

        /**
         * Get the list of properties to group by.
         *
         * @return the list of properties to group by.
         */
        public List<String> getGroupBy() { return groupBy; }

        /**
         * Set GroupBy
         *
         * @param groupBy the group by argument.
         */
        public void setGroupBy(List<String> groupBy) {
            this.groupBy = groupBy;
        }

        /**
         * Set GroupBy. This adds an additional GroupBy argument, and when called
         * multiple times during method chaining, it can set a list of GroupBy's.
         *
         * @param groupBy the group by String.
         * @return This instance (for method chaining).
         */
        public Builder withGroupBy(String groupBy) {
            if (this.groupBy == null) {
                this.groupBy = new ArrayList<String>();
            }
            this.groupBy.add(groupBy);
            return this;
        }

        /**
         * Set the GroupBy
         *
         * @param groupBy the ArrayList of properties to group by.
         * @return This instance (for method chaining).
         */
        public Builder withGroupBy(List<String> groupBy) {
            setGroupBy(groupBy);
            return this;
        }

        /**
         * Get max age
         *
         * @return the max age.
         */
        public Integer getMaxAge() { return maxAge; }

        /**
         * Set max age
         *
         * @param maxAge the max age.
         */
        public void setMaxAge(Integer maxAge) { this.maxAge = maxAge; }

        /**
         * Set max age
         *
         * @param maxAge the max age.
         * @return This instance (for method chaining).
         */
        public Builder withMaxAge(Integer maxAge) {
            setMaxAge(maxAge);
            return this;
        }

        /**
         * Get the percentile
         *
         * @return the percentile.
         */
        public Double getPercentile() { return percentile; }

        /**
         * Set percentile
         *
         * @param percentile the percentile.
         */
        public void setPercentile(Double percentile) { this.percentile = percentile;  }

        /**
         * Set percentile
         *
         * @param percentile the percentile.
         */
        public void setPercentile(Integer percentile) { this.percentile = percentile.doubleValue(); }

        /**
         * Set percentile
         *
         * @param percentile the percentile (type Double).
         * @return This instance (for method chaining).
         */
        public Builder withPercentile(Double percentile) {
            setPercentile(percentile);
            return this;
        }

        /**
         * Set percentile
         *
         * @param percentile the percentile (type Integer).
         * @return This instance (for method chaining).
         */
        public Builder withPercentile(Integer percentile) {
            setPercentile(percentile.doubleValue());
            return this;
        }

        /**
         * Get timeframe
         *
         * @return the timeframe.
         */
        public Timeframe getTimeframe() { return timeframe; }

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
            setTimeframe(timeframe);
            return this;
        }

        /**
         * Build the Query after the method chaining arguments.
         *
         * @return The new Query instance.
         */
        public Query build() {
            Query query = new Query(this);

            if (!query.areParamsValid()) {
                throw new IllegalArgumentException("Keen Query parameters are insufficient. " +
                        "Please check Query API docs for required arguments.");
            }

            return query;
        }
    }
}
