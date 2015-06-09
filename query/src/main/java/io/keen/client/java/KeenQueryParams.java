package io.keen.client.java;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * Created by claireyoung on 5/18/15.
 */
public class KeenQueryParams {

    // required for all queries
    private String eventCollection;

    // required by most queries
    private String targetProperty;

    // optional
    private List<Map<String, Object>> filters;
    private String relativeTimeframe;
    private Map<String, Object> absoluteTimeframe;  // absolute timeframe with "start" and "end" keys
    private String interval;
    private String timezone;
    private String groupBy;
    private Integer maxAge; // integer greater than 30 seconds: https://keen.io/docs/data-analysis/caching/

    // required by the Percentile query
    private Double percentile;  // 0-100 with two decimal places of precision for example, 99.99

    // optional for the Extraction query
    private Integer latest;     // An integer containing the number of most recent events to extract.
    private String email;

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

        if (null != relativeTimeframe) {
            queryArgs.put(KeenQueryConstants.TIMEFRAME, relativeTimeframe);
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
        if (null != absoluteTimeframe && absoluteTimeframe.isEmpty() == false) {
            queryArgs.put(KeenQueryConstants.TIMEFRAME, absoluteTimeframe);
        }

        return queryArgs;
    }

    public void addAbsoluteTimeframe(String start, String end) {
        if (absoluteTimeframe == null) {
            absoluteTimeframe = new HashMap<String, Object>();
        }

        absoluteTimeframe.put(KeenQueryConstants.START, start);
        absoluteTimeframe.put(KeenQueryConstants.END, end);
    }

    public void addFilter(String propertyName, String operator, Object propertyValue) {
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
    protected KeenQueryParams(QueryParamBuilder builder) {
        this.eventCollection = builder.eventCollection;
        this.targetProperty = builder.targetProperty;
        this.relativeTimeframe = builder.timeframe;
        this.interval = builder.interval;
        this.timezone = builder.timezone;
        this.groupBy = builder.groupBy;
        this.maxAge = builder.maxAge;
        this.percentile = builder.percentile;
        this.latest = builder.latest;
        this.email = builder.email;
        this.absoluteTimeframe = builder.absoluteTimeframe;
    }

    public static class QueryParamBuilder {
        private String eventCollection;     // required

        // mostly required
        private String targetProperty;

        // optional
        private List<Map<String, Object>> filters;
        private String timeframe;
        private Map<String, Object> absoluteTimeframe;
        private String interval;
        private String timezone;
        private String groupBy;
        private Integer maxAge;
        private Double percentile;
        private Integer latest;
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


        public String getRelativeTimeframe() {return timeframe;}
        public void setRelativeTimeframe(String timeframe) {this.timeframe = timeframe;}
        public QueryParamBuilder withRelativeTimeframe(String timeframe) {
            setRelativeTimeframe(timeframe);
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

        public Integer getMaxAge() {return maxAge;}
        public void setMaxAge(Integer maxAge) {this.maxAge = maxAge;}
        public QueryParamBuilder withMaxAge(Integer maxAge) {
            setMaxAge(maxAge);
            return this;
        }

        public Double getPercentile() {return percentile;}
        public void setPercentile(Double percentile) {this.percentile = percentile;}
        public QueryParamBuilder withPercentile(Double percentile) {
            setPercentile(percentile);
            return this;
        }

        public Integer getLatest() {return latest;}
        public void setLatest(Integer latest) {this.latest = latest;}
        public QueryParamBuilder withLatest(Integer latest) {
            setLatest(latest);
            return this;
        }

        public String getEmail() {return email;}
        public void setEmail(String email) {this.email = email;}
        public QueryParamBuilder withEmail(String email) {
            setEmail(email);
            return this;
        }

        public Map<String, Object> getAbsoluteTimeframe() {return absoluteTimeframe;}
        public void setAbsoluteTimeframe(Map<String, Object> absoluteTimeframe) {this.absoluteTimeframe = absoluteTimeframe;}
        public QueryParamBuilder withAbsoluteTimeframe(Map<String, Object> absoluteTimeframe) {
            setAbsoluteTimeframe(absoluteTimeframe);
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
