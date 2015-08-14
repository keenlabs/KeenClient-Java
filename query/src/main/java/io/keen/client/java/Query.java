package io.keen.client.java;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * Query represents all the details of the query to be run, including required
 * and optional parameters.
 *
 * Created by claireyoung on 5/18/15.
 * @author claireyoung
 * @since 1.0.0
 */
public class Query {

    private final QueryType queryType;
    private final String eventCollection;
    private final String targetProperty;

    // optional
    private final Timeframe timeframe;
    private final List<Map<String, Object>> filters;
    private final String interval;    // requires timeframe to be set
    private final String timezone;
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
    public Map<String, Object> constructQueryArgs() {

        Map<String, Object> queryArgs = new HashMap<String, Object>();

        if (eventCollection != null) {
            queryArgs.put(KeenQueryConstants.EVENT_COLLECTION, eventCollection);
        }

        if (interval != null) {
            queryArgs.put(KeenQueryConstants.INTERVAL, interval);
        }

        if (timezone != null) {
            queryArgs.put(KeenQueryConstants.TIMEZONE, timezone);
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

        if (filters != null && filters.isEmpty() == false) {
            queryArgs.put(KeenQueryConstants.FILTERS, filters);
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
     * Constructs a Keen Query Params using a builder.
     *
     * @param builder The builder from which to retrieve this client's interfaces and settings.
     */
    protected Query(Builder builder) {
        this.eventCollection = builder.eventCollection;
        this.targetProperty = builder.targetProperty;
        this.interval = builder.interval;
        this.timezone = builder.timezone;
        this.groupBy = builder.groupBy;
        this.maxAge = builder.maxAge;
        this.percentile = builder.percentile;
        this.queryType = builder.queryType;
        this.timeframe = builder.timeframe;
        this.filters = builder.filters;
    }

    /**
     * Builder to construct a query with required and optional arguments.
     * Note that the only required argument in this builder is QueryType, although
     * individual queries may require additional arguments.
     */
    public static class Builder {
        private QueryType queryType;
        private String eventCollection;
        private String targetProperty;

        // required by the Percentile query
        private Double percentile;    // 0-100 with two decimal places of precision for example, 99.99

        // optional
        private Timeframe timeframe;
        private List<Map<String, Object>> filters;
        private String interval;
        private String timezone;
        private ArrayList<String> groupBy;
        private Integer maxAge;

        public Builder(QueryType queryType) {
            this.queryType = queryType;
        }

        /**
         * get filters
         * @return a list of filters.
         */
        public List<Map<String, Object>> getFilters() { return filters; }

        /**
         * set filters
         * @param filters  the filter arguments.
         */
        public void setFilters(List<Map<String, Object>> filters) {this.filters = filters;}
        public Builder withFilters(List<Map<String, Object>> filters) {
            setFilters(filters);
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
         */
        public Builder withFilter(String propertyName, FilterOperator operator, Object propertyValue) {
            Map<String, Object> filter = new HashMap<String, Object>();
            filter.put(KeenQueryConstants.PROPERTY_NAME, propertyName);
            filter.put(KeenQueryConstants.OPERATOR, operator.toString());
            filter.put(KeenQueryConstants.PROPERTY_VALUE, propertyValue);

            if (filters == null) {
                filters = new ArrayList<Map<String, Object>>();
            }

            filters.add(filter);
            return this;
        }

        /**
         * get event collection
         * @return the event collection.
         */
        public String getEventCollection() { return eventCollection; }

        /**
         * Set event collection
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
         * get target property
         * @return the target property.
         */
        public String getTargetProperty() { return targetProperty; }

        /**
         * Set target property
         * @param targetProperty the target property.
         */
        public void setTargetProperty(String targetProperty) { this.targetProperty = targetProperty; }

        /**
         * Set target property
         * @param targetProperty the target property.
         * @return This instance (for method chaining).
         */
        public Builder withTargetProperty(String targetProperty) {
            setTargetProperty(targetProperty);
            return this;
        }

        /**
         * get Interval
         * @return the interval.
         */
        public String getInterval() { return interval; }

        /**
         * Set interval
         * @param interval the interval.
         */
        public void setInterval(String interval) { this.interval = interval; }

        /**
         * Set interval
         * @param interval the interval.
         * @return This instance (for method chaining).
         */
        public Builder withInterval(String interval) {
            setInterval(interval);
            return this;
        }

        /**
         * get timezone
         * @return the timezone.
         */
        public String getTimezone() { return timezone; }

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
        public Builder withTimezone(String timezone) {
            setTimezone(timezone);
            return this;
        }

        /**
         * get the list of properties to group by.
         * @return the list of properties to group by.
         */
        public ArrayList<String> getGroupBy() {return groupBy;}

        /**
         * Set group by
         * @param groupBy the group by argument.
         */
        public void setGroupBy(ArrayList<String> groupBy) {
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
         * @param groupBy the ArrayList of properties to group by.
         * @return This instance (for method chaining).
         */
        public Builder withGroupBy(ArrayList<String> groupBy) {
            setGroupBy(groupBy);
            return this;
        }

        /**
         * get max age
         * @return the max age.
         */
        public Integer getMaxAge() { return maxAge; }

        /**
         * Set max age
         * @param maxAge the max age.
         */
        public void setMaxAge(Integer maxAge) { this.maxAge = maxAge; }

        /**
         * Set max age
         * @param maxAge the max age.
         * @return This instance (for method chaining).
         */
        public Builder withMaxAge(Integer maxAge) {
            setMaxAge(maxAge);
            return this;
        }

        /**
         * get the percentile
         * @return the percentile.
         */
        public Double getPercentile() { return percentile; }

        /**
         * Set percentile
         * @param percentile the percentile.
         */
        public void setPercentile(Double percentile) { this.percentile = percentile;  }

        /**
         * Set percentile
         * @param percentile the percentile.
         */
        public void setPercentile(Integer percentile) { this.percentile = percentile.doubleValue(); }

        /**
         * Set percentile
         * @param percentile the percentile (type Double).
         * @return This instance (for method chaining).
         */
        public Builder withPercentile(Double percentile) {
            setPercentile(percentile);
            return this;
        }

        /**
         * Set percentile
         * @param percentile the percentile (type Integer).
         * @return This instance (for method chaining).
         */
        public Builder withPercentile(Integer percentile) {
            setPercentile(percentile.doubleValue());
            return this;
        }

        /**
         * get timeframe
         * @return the timeframe.
         */
        public Timeframe getTimeframe() { return timeframe; }

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
        public Builder withTimeframe(Timeframe timeframe) {
            setTimeframe(timeframe);
            return this;
        }

        /**
         * Build the Query after the method chaining arguments.
         * */
        public Query build() {
            // we can do initialization here, but it's ok if everything is null.
            return new Query(this);
        }
    }
}
