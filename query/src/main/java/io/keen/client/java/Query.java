package io.keen.client.java;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * Created by claireyoung on 5/18/15.
 */
public class Query {

    private QueryType queryType;

    // required for all queries
    private String eventCollection;

    // required by most queries
    private String targetProperty;

    // optional
    private List<Map<String, Object>> filters;
//    private String relativeTimeframe;
//    private Map<String, Object> absoluteTimeframe;  // absolute timeframe with "start" and "end" keys
//    Timeframe timeframe;
    private String interval;    // requires timeframe to be set
    private String timezone;
    private ArrayList<String> groupBy;
    private Integer maxAge; // integer greater than 30 seconds: https://keen.io/docs/data-analysis/caching/

    // required by the Percentile query
    private Double percentile;  // 0-100 with two decimal places of precision for example, 99.99

    // optional for the Extraction query
    private Integer latest;     // An integer containing the number of most recent events to extract.
    private String email;
    private String contentEncoding; // TODO add these
    private String contentType;     // TODO add these
    private List<String> propertyNames; // TODO add these

    private Map<String, Object> analyses;      // required for Multi-Analysis
    private List<Map<String, Object>> funnelSteps;  // required for funnel

    /**
     * Constructs the map to pass to the JSON handler, so that the proper required
     * and optional Query arguments can be sent out to the server.
     *
     * @return The JSON object map.
     */
    public Map<String, Object> ConstructQueryArgs() {

        Map<String, Object> queryArgs = new HashMap<String, Object>();

        StringBuffer queryString = new StringBuffer();
        if (null != eventCollection) {
            queryArgs.put(KeenQueryConstants.EVENT_COLLECTION, eventCollection);
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

        if (null != analyses && analyses.isEmpty() == false) {
            queryArgs.put(KeenQueryConstants.ANALYSES, analyses);
        }
        if (null != funnelSteps && funnelSteps.isEmpty() == false) {
            queryArgs.put(KeenQueryConstants.STEPS, funnelSteps);
        }

//        if (null != relativeTimeframe) {
//            queryArgs.put(KeenQueryConstants.TIMEFRAME, relativeTimeframe);
//        }
//
//        if (null != absoluteTimeframe && absoluteTimeframe.isEmpty() == false) {
//            queryArgs.put(KeenQueryConstants.TIMEFRAME, absoluteTimeframe);
//        }

//        if (null != timeframe) {
//            Map<String, Object> timeframeArgs = timeframe.constructTimeframeArgs();
//            queryArgs.putAll(timeframeArgs);
//        }
        return queryArgs;
    }

    public QueryType getQueryType() {
        return this.queryType;
    }

    public boolean hasGroupBy() {return groupBy != null;}

    public boolean hasInterval() {return interval != null;}

    /**
     * Sets the start and end of the absolute time frame.
     * Refer to https://keen.io/docs/data-analysis/timeframe/#absolute-timeframes
     *
     * @param start     The start of the time frame in ISO-8601 Format.
     * @param end       The end of the time frame in ISO-8601 Format.
     */
//    public void addAbsoluteTimeframe(String start, String end) {
//        if (timeframe == null) {
//            timeframe = new Timeframe();
//        }
//        timeframe.setTimeframe(start, end);
//    }

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
    /**
     * Verifies whether the parameters are valid, based on the input query name.
     *
     * @param queryType     The type of the query (in {@link QueryType}).
     * @return boolean      whether the parameters are valid.
     */
    public boolean AreParamsValid(QueryType queryType) {

        if (queryType == QueryType.COUNT_RESOURCE || queryType == QueryType.EXTRACTION_RESOURCE
                || queryType == QueryType.MULTI_ANALYSIS) {
            if (eventCollection == null || eventCollection.isEmpty()) {
                return false;
            }
        }

        if (queryType == QueryType.COUNT_UNIQUE
                || queryType == QueryType.MINIMUM_RESOURCE || queryType == QueryType.MAXIMUM_RESOURCE
                || queryType == QueryType.AVERAGE_RESOURCE || queryType == QueryType.MEDIAN_RESOURCE
                || queryType == QueryType.PERCENTILE_RESOURCE || queryType == QueryType.SUM_RESOURCE
                || queryType == QueryType.SELECT_UNIQUE_RESOURCE) {

            if (eventCollection == null || eventCollection.isEmpty() || targetProperty == null || targetProperty.isEmpty()) {
                return false;
            }
        }

        if (queryType == QueryType.PERCENTILE_RESOURCE) {
            if (percentile == null) {
                return false;
            }
        }

        if (queryType == QueryType.FUNNEL) {
            if (funnelSteps == null || funnelSteps.isEmpty()) {
                return false;
            }
        }

        if (queryType == QueryType.MULTI_ANALYSIS) {
            if (analyses == null || analyses.isEmpty()) {
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
    protected Query(QueryBuilder builder) {
        this.eventCollection = builder.eventCollection;
        this.targetProperty = builder.targetProperty;
        this.interval = builder.interval;
        this.timezone = builder.timezone;
        this.groupBy = builder.groupBy;
        this.maxAge = builder.maxAge;
        this.percentile = builder.percentile;
        this.latest = builder.latest;
        this.email = builder.email;
//        this.relativeTimeframe = builder.timeframe;
//        this.absoluteTimeframe = builder.absoluteTimeframe;
//        this.timeframe = builder.timeframe;
        this.funnelSteps = builder.funnelSteps;
        this.analyses = builder.analyses;
        this.queryType = builder.queryType;
    }

    public static class QueryBuilder {
        private QueryType queryType;

        private String eventCollection;     // required

        // mostly required
        private String targetProperty;

        private Double percentile;

        // optional
        private List<Map<String, Object>> filters;
//        private Timeframe timeframe;
        private String interval;
        private String timezone;
        private ArrayList<String> groupBy;
        private Integer maxAge;
        private Integer latest;
        private String email;

        private Map<String, Object> analyses;      // required for Multi-Analysis
        private List<Map<String, Object>> funnelSteps;  // required for funnel

        public QueryBuilder(QueryType queryType) {
            this.queryType = queryType;
        }

        public Map<String, Object> getAnalyses() {return analyses;}
        public void setAnalyses(Map<String, Object> analyses) {this.analyses = analyses;}
        public QueryBuilder withAnalyses(Map<String, Object> analyses) {
            setAnalyses(analyses);
            return this;
        }

        public List<Map<String, Object>> setFunnelSteps() {return funnelSteps;}
        public void setFunnelSteps(List<Map<String, Object>> funnelSteps) {this.funnelSteps = funnelSteps;}
        public QueryBuilder withFunnelSteps(List<Map<String, Object>> funnelSteps) {
            setFunnelSteps(funnelSteps);
            return this;
        }

        public List<Map<String, Object>> setFilters() {return filters;}
        public void setFilters(List<Map<String, Object>> filters) {this.filters = filters;}
        public QueryBuilder withFilters(List<Map<String, Object>> filters) {
            setFilters(filters);
            return this;
        }

        public String getEventCollection() {return eventCollection;}
        public void setEventCollection(String eventCollection) {this.eventCollection = eventCollection;}
        public QueryBuilder withEventCollection(String eventCollection) {
            setEventCollection(eventCollection);
            return this;
        }

        public String getTargetProperty() {return targetProperty;}
        public void setTargetProperty(String targetProperty) {this.targetProperty = targetProperty;}
        public QueryBuilder withTargetProperty(String targetProperty) {
            setTargetProperty(targetProperty);
            return this;
        }

        public String getInterval() {return interval;}
        public void setInterval(String interval) {this.interval = interval;}
        public QueryBuilder withInterval(String interval) {
            setInterval(interval);
            return this;
        }

        public String getTimezone() {return timezone;}
        public void setTimezone(String timezone) {this.timezone = timezone;}
        public QueryBuilder withTimezone(String timezone) {
            setTimezone(timezone);
            return this;
        }

        public ArrayList<String> getGroupBy() {return groupBy;}
        public void setGroupBy(ArrayList<String> groupBy) {
            this.groupBy = groupBy;
        }
        public QueryBuilder withGroupBy(String groupBy) {
            ArrayList<String> groupByList = new ArrayList<String>();
            groupByList.add(groupBy);
            setGroupBy(groupByList);
            return this;
        }
        public QueryBuilder withGroupBy(ArrayList<String> groupBy) {
            setGroupBy(groupBy);
            return this;
        }

        public Integer getMaxAge() {return maxAge;}
        public void setMaxAge(Integer maxAge) {this.maxAge = maxAge;}
        public QueryBuilder withMaxAge(Integer maxAge) {
            setMaxAge(maxAge);
            return this;
        }

        public Double getPercentile() {return percentile;}
        public void setPercentile(Double percentile) {this.percentile = percentile;}
        public void setPercentile(Integer percentile) {this.percentile = percentile.doubleValue();}
        public QueryBuilder withPercentile(Double percentile) {
            setPercentile(percentile);
            return this;
        }
        public QueryBuilder withPercentile(Integer percentile) {
            setPercentile(percentile.doubleValue());
            return this;
        }

        public Integer getLatest() {return latest;}
        public void setLatest(Integer latest) {this.latest = latest;}
        public QueryBuilder withLatest(Integer latest) {
            setLatest(latest);
            return this;
        }

        public String getEmail() {return email;}
        public void setEmail(String email) {this.email = email;}
        public QueryBuilder withEmail(String email) {
            setEmail(email);
            return this;
        }

//        public Map<String, Object> getAbsoluteTimeframe() {return absoluteTimeframe;}
//        public void setAbsoluteTimeframe(Map<String, Object> absoluteTimeframe) {this.absoluteTimeframe = absoluteTimeframe;}
//        public QueryBuilder withAbsoluteTimeframe(Map<String, Object> absoluteTimeframe) {
//            setAbsoluteTimeframe(absoluteTimeframe);
//            return this;
//        }

//        public String getRelativeTimeframe() {return timeframe;}
//        public void setRelativeTimeframe(String timeframe) {this.timeframe = timeframe;}
//        public QueryBuilder withRelativeTimeframe(String timeframe) {
//            setRelativeTimeframe(timeframe);
//            return this;
//        }

//        public void setTimeframe(String relativeTimeframe) {
//            timeframe = new Timeframe(relativeTimeframe);
//        }
//
//        public void setTimeframe(String start, String end) {
//            timeframe = new Timeframe(start, end);
//        }
//
//        public void setTimeframe(Timeframe timeframe) { this.timeframe = timeframe;}
//
//        public QueryBuilder withTimeframe(String timeframe) {
//            setTimeframe(timeframe);
//            return this;
//        }
//
//        public QueryBuilder withTimeframe(String start, String end) {
//            setTimeframe(start, end);
//            return this;
//        }
//
//        public QueryBuilder withTimeframe(Timeframe timeframe) {
//            setTimeframe(timeframe);
//            return this;
//        }

        public Query build() {
            // we can do initialization here, but it's ok if everything is null.
            return buildInstance();
        }

        protected Query buildInstance() {
            return new Query(this);
        }

    }

}
