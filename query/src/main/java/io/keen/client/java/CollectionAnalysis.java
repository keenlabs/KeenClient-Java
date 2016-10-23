package io.keen.client.java;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import io.keen.client.java.exceptions.KeenQueryClientException;

/**
 * Represents the types of analyses that target a single event collection. Single Analysis and
 * Multi-Analysis are examples of this, whereas Funnel is not.
 * 
 * @author masojus
 */
abstract class CollectionAnalysis extends KeenQueryRequest {
    // required
    private final String collectionName;
    private final Timeframe timeframe;

    // optional
    private final RequestParameterCollection<Filter> filters;
    private final String interval;
    private final Collection<String> groupBy;


    /**
     * @return Whether or not this analysis has a GroupBy specified.
     */
    public boolean hasGroupBy() { return null != this.groupBy; }

    /**
     * @return Whether or not this analysis has an Interval specified.
     */
    public boolean hasInterval() { return null != this.interval; }

    @Override
    boolean groupedResponseExpected() {
        return hasGroupBy();
    }

    @Override
    boolean intervalResponseExpected() {
        return hasInterval();
    }

    @Override
    Map<String, Object> constructRequestArgs() {
        Map<String, Object> analysisArgs = new HashMap<String, Object>();

        // The Builder should have validated the individual properties as they were configured,
        // and a call to validateParams() during build() would have thrown if inter-parameter
        // rules were broken, so if optional things are non-null, we should be OK to add them.

        analysisArgs.put(KeenQueryConstants.EVENT_COLLECTION, this.collectionName);
        analysisArgs.putAll(this.timeframe.constructTimeframeArgs());

        // Technically this could add an empty "'filters': []," to the request, but that's fine.
        // We can fix that later by checking if the RequestParameterCollection is empty, or by
        // changing RequestParameter's interface to take the outer Map<> as a parameter and add
        // things only as needed.
        if (null != this.filters) {
            analysisArgs.put(KeenQueryConstants.FILTERS,
                    this.filters.constructParameterRequestArgs());
        }

        if (null != this.interval) {
            analysisArgs.put(KeenQueryConstants.INTERVAL, this.interval);
        }

        if (null != this.groupBy) {
            analysisArgs.put(KeenQueryConstants.GROUP_BY, this.groupBy);
        }

        return analysisArgs;
    }

    protected CollectionAnalysis(Builder builder) {
        this.collectionName = builder.collectionName;
        this.timeframe = builder.timeframe;
        this.interval = builder.interval;
        this.groupBy = builder.groupBy;

        if (null != builder.filters && !builder.filters.isEmpty())
        {
            this.filters = new RequestParameterCollection<Filter>(builder.filters);
        }
        else
        {
            this.filters = null;
        }
    }

    /**
     * Ensures that all the parameters provided by the Builder when constructing this analysis
     * follow the rules set forth in the <a href="https://keen.io/docs/api/#analyses">API Docs</a>.
     *
     * Some parameters require others, some are required always for certain analysis types, and
     * some things are forbidden or just don't make sense, which could indicate an error in client
     * code that needs addressing.
     *
     * @throws KeenQueryClientException if validation fails with specific reason for failure.
     */
    protected void validateParams() throws KeenQueryClientException {
        // Event collection name is required.
        if (null == this.collectionName || this.collectionName.trim().isEmpty()) {
            throw new KeenQueryClientException(
                    "There must be an event collection name set to perform a CollectionAnalysis.");
        }

        // Timeframe is required.
        if (null == this.timeframe) {
            throw new KeenQueryClientException(
                    "A 'timeframe' parameter is required for a CollectionAnalysis");
        }

        // Group by does not supper geo-filtering.
        if (null != this.groupBy && null != this.filters) {
            for (Filter filter : this.filters) {
                if (filter.isGeoFilter()) {
                    throw new KeenQueryClientException("The 'group_by' parameter cannot be used " +
                            "in conjunction with geo-filtering.");
                }
            }
        }
    }

    /**
     * An abstract base builder class that knows how to help add appropriate parameters for
     * building a CollectionAnalysis instance.
     *
     * The expectation is that this is subclassed in places where we need to build something
     * that extends CollectionAnalysis. The fluent methods call into the abstract getThis() in
     * order to return 'this' cast to the correct derived type (ChainT). That way client code won't
     * accidentally get back 'this' as a CollectionAnalysis.Builder type, which would have
     * limited functionality.
     *
     * We could instead make these protected and force descendant classes to pick exactly which
     * ones to make public, and therefore be in charge of returning the Builder subclass
     * themselves, but for now they are all useful for subclasses.
     *
     * @param <ChainT> The type to return from fluent methods for chaining. Likely a derived builder
     *                 type so as to continue chaining with the full capabilities of the subclass.
     */
    protected abstract static class Builder <ChainT> {
        // required
        private String collectionName;
        private Timeframe timeframe;

        // optional
        private Collection<Filter> filters;
        private String interval;
        private Collection<String> groupBy;


        /**
         * Get the event collection's name.
         *
         * @return The event collection's name.
         */
        public String getCollectionName() { return this.collectionName; }

        /**
         * Set the event collection's name. This will replace the existing collection name.
         *
         * @param collectionName The event collection's name.
         */
        public void setCollectionName(String collectionName) {
            if (null == collectionName || collectionName.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "The 'collectionName' parameter is null or empty.");
            }

            this.collectionName = collectionName;
        }

        /**
         * Set the event collection's name. This should only happen once.
         *
         * @param collectionName The event collection's name.
         * @return This instance (for method chaining).
         */
        public ChainT withCollectionName(String collectionName) {
            if (null != this.collectionName) {
                throw new IllegalArgumentException("Attempting to set the 'collectionName' " +
                        "parameter for this analysis multiple times, but there can be only one.");
            }

            setCollectionName(collectionName);
            return getThis();
        }

        /**
         * Get the timeframe.
         *
         * @return The timeframe for this analysis.
         */
        public Timeframe getTimeframe() { return this.timeframe; }

        /**
         * Set the timeframe. This will replace the existing timeframe.
         *
         * @param timeframe The timeframe for this analysis.
         */
        public void setTimeframe(Timeframe timeframe) { this.timeframe = timeframe; }

        /**
         * Set the timeframe. This should only happen once.
         *
         * @param timeframe The timeframe for this analysis.
         * @return This instance (for method chaining).
         */
        public ChainT withTimeframe(Timeframe timeframe) {
            if (null != this.timeframe) {
                throw new IllegalArgumentException("Attempting to set the 'timeframe' parameter " +
                        "for this analysis multiple times, but there can be only one.");
            }

            setTimeframe(timeframe);
            return getThis();
        }

        /**
         * Get the filters for this analysis.
         *
         * @return The collection of filters.
         */
        public Collection<Filter> getFilters() { return this.filters; }

        /**
         * Set the list of filters. Existing filters will be discarded.
         * Refer to the <a href="https://keen.io/docs/api/#filters">API documentation</a>.
         *
         * @param filters The new filter arguments to replace the discarded ones, if any.
         */
        public void setFilters(Collection<? extends Filter> filters) {
            this.filters = null;

            // Client code may just be clearing all the filters.
            if (null != filters) {
                withFilters(filters);  // Shallow copy the list of steps
            }
        }

        /**
         * Add the given filters as optional parameters to the analysis.
         * Refer to the <a href="https://keen.io/docs/api/#filters">API documentation</a>.
         *
         * @param filters The new filter arguments to add to the existing list of filters.
         * @return This instance (for method chaining).
         */
        public ChainT withFilters(Collection<? extends Filter> filters) {
            if (null == filters) {
                throw new IllegalArgumentException("The 'filters' parameter cannot be null.");
            }

            // Add each filter to the list of filters, appending to anything
            // that already exists.
            for (Filter filter : filters) {
                addFilter(filter);
            }

            return getThis();
        }

        /**
         * Add a filter as an optional parameter to the analysis.
         * Refer to the <a href="https://keen.io/docs/api/#filters">API documentation</a>.
         *
         * @param propertyName  The name of the property.
         * @param operator      The operator (eg., gt, lt, exists, contains)
         * @param propertyValue The property value. Refer to API documentation for info.
         *                      This can be a string, number, boolean, or geo-coordinates
         *                      and is based on what the operator is.
         * @return This instance (for method chaining).
         */
        public ChainT withFilter(String propertyName,
                                  FilterOperator operator,
                                  Object propertyValue) {
            addFilter(propertyName, operator, propertyValue);
            return getThis();
        }

        /**
         * Add a filter as an optional parameter to the analysis.
         * Refer to the <a href="https://keen.io/docs/api/#filters">API documentation</a>.
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
         * Add a filter as an optional parameter to the analysis.
         * Refer to the <a href="https://keen.io/docs/api/#filters">API documentation</a>.
         *
         * @param filter The filter to add to the existing list of filters.
         * @return This instance (for method chaining).
         */
        public ChainT withFilter(Filter filter) {
            addFilter(filter);
            return getThis();
        }

        /**
         * Add a filter as an optional parameter to the analysis.
         * Refer to the <a href="https://keen.io/docs/api/#filters">API documentation</a>.
         *
         * @param filter The filter to add to the existing list of filters.
         */
        public void addFilter(Filter filter) {
            if (null == filter) {
                throw new IllegalArgumentException("The 'filter' parameter cannot be null.");
            }

            if (null == this.filters) {
                this.filters = new LinkedList<Filter>();
            }

            this.filters.add(filter);
        }

        /**
         * Get the interval for this analysis.
         *
         * @return The interval.
         */
        public String getInterval() { return this.interval; }

        /**
         * Set the interval for this analysis. This should only happen once.
         *
         * @param interval The interval.
         */
        public void setInterval(String interval) {
            if (null == interval || interval.trim().isEmpty()) {
                throw new IllegalArgumentException("The 'interval' parameter is null or empty.");
            }

            this.interval = interval;
        }

        /**
         * Set the interval for this analysis.
         *
         * @param interval The interval.
         * @return This instance (for method chaining).
         */
        public ChainT withInterval(String interval) {
            if (null != this.interval) {
                throw new IllegalArgumentException("Attempting to set the 'interval' parameter " +
                    "for this analysis multiple times, but there can be only one.");
            }

            setInterval(interval);
            return getThis();
        }

        /**
         * Get the list of properties to group by.
         *
         * @return The group by parameters.
         */
        public Collection<String> getGroupBy() { return this.groupBy; }

        /**
         * Set the list of properties to group by. Existing group by properties will be discarded.
         *
         * @param groupBy The group by parameters to append.
         */
        public void setGroupBy(Collection<String> groupBy) {
            this.groupBy = null;

            // Client code may just be clearing all the group by properties.
            if (null != groupBy) {
                withGroupBy(groupBy);
            }
        }

        /**
         * Adds a list of properties to group by to the list of existing group by properties.
         *
         * @param groupBy The group by parameters to append.
         * @return This instance (for method chaining).
         */
        public ChainT withGroupBy(Collection<String> groupBy) {
            if (null == groupBy) {
                throw new IllegalArgumentException("The 'groupBy' parameter cannot be null.");
            }

            // Add each group by property to the list, appending to anything that already exists.
            for (String groupByProperty : groupBy) {
                addGroupBy(groupByProperty);
            }

            return getThis();
        }

        /**
         * Adds a group by property to the list of existing group by properties.
         *
         * @param groupBy The group by parameter.
         * @return This instance (for method chaining).
         */
        public ChainT withGroupBy(String groupBy) {
            addGroupBy(groupBy);
            return getThis();
        }

        /**
         * Adds a group by property to the list of existing group by properties.
         *
         * @param groupBy The group by parameter.
         */
        public void addGroupBy(String groupBy) {
            if (null == groupBy || groupBy.trim().isEmpty()) {
                throw new IllegalArgumentException("The 'groupBy' parameter is null or empty.");
            }

            if (null == this.groupBy) {
                this.groupBy = new HashSet<String>(); // No point in specifying one twice.
            }

            this.groupBy.add(groupBy);
        }

        protected abstract ChainT getThis();
    }
}
