package io.keen.client.java;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DatasetQuery {

    private String projectId;

    private String analysisType;

    private String eventCollection;

    private String timezone;

    private String interval;

    private String timeframe;

    private List<String> groupBy;

    private List<Filter> filters;

    static DatasetQuery fromMap(Map<String, Object> properties) {
        DatasetQuery query = new DatasetQuery();
        query.projectId = (String) properties.get("project_id");
        query.analysisType = (String) properties.get("analysis_type");
        query.eventCollection = (String) properties.get("event_collection");
        query.timezone = (String) properties.get("timezone");
        query.interval = (String) properties.get("interval");
        query.timeframe = (String) properties.get("timeframe");
        //noinspection unchecked
        query.groupBy = (List<String>) properties.get("group_by");

        if (properties.get("filters") != null) {
            query.filters = new LinkedList<Filter>();
            //noinspection unchecked
            for (Map<String, Object> filter : (List<Map<String, Object>>) properties.get("filters")) {
                query.filters.add(new Filter(
                        (String) filter.get("property_name"),
                        FilterOperator.fromString((String) filter.get("operator")),
                        filter.get("property_value")
                ));
            }
        }
        return query;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getAnalysisType() {
        return analysisType;
    }

    public void setAnalysisType(String analysisType) {
        this.analysisType = analysisType;
    }

    public String getEventCollection() {
        return eventCollection;
    }

    public void setEventCollection(String eventCollection) {
        this.eventCollection = eventCollection;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public String getInterval() {
        return interval;
    }

    public void setInterval(String interval) {
        this.interval = interval;
    }

    public String getTimeframe() {
        return timeframe;
    }

    public void setTimeframe(String timeframe) {
        this.timeframe = timeframe;
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<String> groupBy) {
        this.groupBy = groupBy;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
    }

    Map<String, Object> asMap() {
//        private List<String> groupBy;
//        private List<Filter> filters;
        Map<String, Object> result = new HashMap<String, Object>();
        if (analysisType != null) {
            result.put(KeenQueryConstants.ANALYSIS_TYPE, analysisType);
        }
        if (eventCollection != null) {
            result.put(KeenQueryConstants.EVENT_COLLECTION, eventCollection);
        }
        if (timeframe != null) {
            result.put(KeenQueryConstants.TIMEFRAME, timeframe);
        }
        if (timezone != null) {
            result.put(KeenQueryConstants.TIMEZONE, timezone);
        }
        if (interval != null) {
            result.put(KeenQueryConstants.INTERVAL, interval);
        }
        return result;
    }

    @Override
    public String toString() {
        return "DatasetQuery{" +
                "projectId='" + projectId + '\'' +
                ", analysisType='" + analysisType + '\'' +
                ", eventCollection='" + eventCollection + '\'' +
                ", timezone='" + timezone + '\'' +
                ", interval='" + interval + '\'' +
                ", timeframe='" + timeframe + '\'' +
                ", groupBy=" + groupBy +
                ", filters=" + filters +
                '}';
    }
}
