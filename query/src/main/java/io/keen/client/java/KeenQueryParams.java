package io.keen.client.java;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * Created by claireyoung on 5/18/15.
 */
public class KeenQueryParams {
//    public String queryName;   // required

//    public String apiKey;      // if we use KeenClient

    // TODO: member variables that should be numeric.

    private String eventCollection;     // required

    // mostly required
    private String targetProperty;

    // optional
    private List<Map<String, Object>> filters;
    private String timeframe;    // todo research this. Absolute timeframe. This needs to be a Map, if it's an absolute timeframe.
                                // withAbsoluteTimeframe vs. withRelativeTimeframe (2 different member vars)
    private String interval;
    private String timezone;
    private String groupBy;
    private String maxAge;   // todo long

    private String percentile;   // todo double
    private String latest;       // long or int

    private String email;

    public KeenQueryParams() {
    }

    public void setEventCollectionAndTargetProperty(String eventCollection, String targetProperty) {
        this.eventCollection = eventCollection;
        this.targetProperty = targetProperty;
    }

    public Map<String, Object> ConstructQueryArgs() {

        Map<String, Object> queryArgs = new HashMap<String, Object>();

        StringBuffer queryString = new StringBuffer();
        if (null != eventCollection) {
            queryArgs.put(KeenQueryConstants.EVENT_COLLECTION, eventCollection);
        }

        if (null != timeframe) {
            queryArgs.put(KeenQueryConstants.TIMEFRAME, timeframe);
        }

        if (null != interval) {
            queryArgs.put(KeenQueryConstants.INTERVAL, interval);
        }

        if (null != timezone) {
            queryArgs.put(KeenQueryConstants.TIMEZONE, timezone);
        }

        if (null != groupBy) {
            queryArgs.put(KeenQueryConstants.GROUP_BY, groupBy);
        }

        if (null != maxAge) {
            queryArgs.put(KeenQueryConstants.MAX_AGE, maxAge);
        }

        if (null != targetProperty) {
            queryArgs.put(KeenQueryConstants.TARGET_PROPERTY, targetProperty);
        }

        if (null != percentile) {
            queryArgs.put(KeenQueryConstants.PERCENTILE, percentile);
        }

        if (null != latest) {
            queryArgs.put(KeenQueryConstants.LATEST, latest);
        }

        if (null != email) {
            queryArgs.put(KeenQueryConstants.EMAIL, email);
        }
        if (null != filters && filters.isEmpty() == false) {
            queryArgs.put(KeenQueryConstants.FILTERS, filters);
        }

        return queryArgs;
    }

    // TODO: should we validate that the filter params are valid???
    public void addFilter(String propertyName, String operator, String propertyValue) {
        Map<String, Object> filter = new HashMap<String, Object>();
        filter.put(KeenQueryConstants.PROPERTY_NAME, propertyName);
        filter.put(KeenQueryConstants.OPERATOR, operator);
        filter.put(KeenQueryConstants.PROPERTY_VALUE, propertyValue);

        if (filters == null) {
            filters = new ArrayList<Map<String, Object>>();
        }

        filters.add(filter);
    }

    // TODO: maybe we can use a reusable library - this method won't be good in long-term
    public boolean AreParamsValid(String queryName) {
        if (queryName.isEmpty()) {
            return false;
        }

        if (queryName.contentEquals(KeenQueryConstants.COUNT_RESOURCE) || queryName.contentEquals(KeenQueryConstants.EXTRACTION_RESOURCE)) {
            if (eventCollection.isEmpty()) {
                return false;
            }
        }

        if (queryName.contentEquals(KeenQueryConstants.COUNT_UNIQUE)
                || queryName.contentEquals(KeenQueryConstants.MINIMUM_RESOURCE) || queryName.contentEquals(KeenQueryConstants.MAXIMUM_RESOURCE)
                || queryName.contentEquals(KeenQueryConstants.AVERAGE_RESOURCE) || queryName.contentEquals(KeenQueryConstants.MEDIAN_RESOURCE)
                || queryName.contentEquals(KeenQueryConstants.PERCENTILE_RESOURCE) || queryName.contentEquals(KeenQueryConstants.SUM_RESOURCE)
                || queryName.contentEquals(KeenQueryConstants.SELECT_UNIQUE_RESOURCE)) {
            if (eventCollection.isEmpty()) {
                return false;
            }
            if (targetProperty.isEmpty()) {
                return false;
            }
        }

        if (queryName.contentEquals(KeenQueryConstants.PERCENTILE_RESOURCE)) {
            if (percentile.isEmpty()) {
                return false;
            }
        }

        // TODO: funnel, multi-analysis

        return true;
    }

    /**
     * Constructs a Keen Query Params using a builder.
     *
     * @param builder The builder from which to retrieve this client's interfaces and settings.
     */
    protected KeenQueryParams(QueryParamBuilder builder) {
        this.eventCollection = builder.eventCollection;
        this.targetProperty = builder.targetProperty;
        this.timeframe = builder.timeframe;
        this.interval = builder.interval;
        this.timezone = builder.timezone;
        this.groupBy = builder.groupBy;
        this.maxAge = builder.maxAge;
        this.percentile = builder.percentile;
        this.latest = builder.latest;
        this.email = builder.email;
    }

    public static class QueryParamBuilder {
        private String eventCollection;     // required

        // mostly required
        private String targetProperty;

        // optional
        private List<Map<String, Object>> filters;
        private String timeframe;
        private String interval;
        private String timezone;
        private String groupBy;
        private String maxAge;
        private String percentile;
        private String latest;
        private String email;

        public List<Map<String, Object>> setFilters() {return filters;}
        public void setFilters(List<Map<String, Object>> filters) {this.filters = filters;}
        public QueryParamBuilder withFilters(List<Map<String, Object>> filters) {
            setFilters(filters);
            return this;
        }

        public String getEventCollection() {return eventCollection;}
        public void setEventCollection(String eventCollection) {this.eventCollection = eventCollection;}
        public QueryParamBuilder withEventCollection(String eventCollection) {
            setEventCollection(eventCollection);
            return this;
        }

        public String getTargetProperty() {return targetProperty;}
        public void setTargetProperty(String targetProperty) {this.targetProperty = targetProperty;}
        public QueryParamBuilder withTargetProperty(String targetProperty) {
            setTargetProperty(targetProperty);
            return this;
        }


        public String getTimeframe() {return timeframe;}
        public void setTimeframe(String timeframe) {this.timeframe = timeframe;}
        public QueryParamBuilder withTimeframe(String timeframe) {
            setTimeframe(timeframe);
            return this;
        }

        public String getInterval() {return interval;}
        public void setInterval(String interval) {this.interval = interval;}
        public QueryParamBuilder withInterval(String interval) {
            setInterval(interval);
            return this;
        }

        public String getTimezone() {return timezone;}
        public void setTimezone(String timezone) {this.timezone = timezone;}
        public QueryParamBuilder withTimezone(String timezone) {
            setTimezone(timezone);
            return this;
        }

        public String getGroupBy() {return groupBy;}
        public void setGroupBy(String groupBy) {this.groupBy = groupBy;}
        public QueryParamBuilder withGroupBy(String groupBy) {
            setGroupBy(groupBy);
            return this;
        }

        public String getMaxAge() {return maxAge;}
        public void setMaxAge(String maxAge) {this.maxAge = maxAge;}
        public QueryParamBuilder withMaxAge(String maxAge) {
            setMaxAge(maxAge);
            return this;
        }

        public String getPercentile() {return percentile;}
        public void setPercentile(String percentile) {this.percentile = percentile;}
        public QueryParamBuilder withPercentile(String percentile) {
            setPercentile(percentile);
            return this;
        }

        public String getLatest() {return latest;}
        public void setLatest(String latest) {this.latest = latest;}
        public QueryParamBuilder withLatest(String latest) {
            setLatest(latest);
            return this;
        }

        public String getEmail() {return email;}
        public void setEmail(String email) {this.email = email;}
        public QueryParamBuilder withEmail(String email) {
            setEmail(email);
            return this;
        }

        public KeenQueryParams build() {
            // we can do initialization here, but it's ok if everything is null.
            return buildInstance();
        }

        protected KeenQueryParams buildInstance() {
            return new KeenQueryParams(this);
        }

    }

}
