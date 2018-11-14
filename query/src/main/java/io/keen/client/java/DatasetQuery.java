package io.keen.client.java;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.*;

import static io.keen.client.java.KeenQueryConstants.*;

public class DatasetQuery {

    private String projectId;

    private String analysisType;

    private List<SubAnalysis> analyses;

    private String targetProperty;

    private String eventCollection;

    private String timezone;

    private String interval;

    private String timeframe;

    private List<String> groupBy;

    private List<Filter> filters;

    @SuppressWarnings("unchecked")
    static DatasetQuery fromMap(Map<String, ?> properties) {
        DatasetQuery query = new DatasetQuery();
        query.projectId = (String) properties.get("project_id");
        query.analysisType = (String) properties.get("analysis_type");
        query.eventCollection = (String) properties.get("event_collection");
        query.timezone = (String) properties.get("timezone");
        query.interval = (String) properties.get("interval");
        query.timeframe = (String) properties.get("timeframe");
        query.groupBy = (List<String>) properties.get("group_by");

        if (properties.get("analyses") != null) {
            query.analyses = new LinkedList<SubAnalysis>();
            Map<String, Map<String, ?>> analysesList = (Map<String, Map<String, ?>>) properties.get("analyses");
            for (Map.Entry<String, Map<String, ?>> analysis : analysesList.entrySet()) {
                query.analyses.add(mapToAnalysis(analysis));
            }
        }

        if (properties.get("filters") != null) {
            query.filters = new LinkedList<Filter>();
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

    private static SubAnalysis mapToAnalysis(Map.Entry<String, Map<String, ?>> analysis) {
        String label = analysis.getKey();
        QueryType analysisType = QueryType.valueOfIgnoreCase((String) analysis.getValue().get("analysis_type"));
        String targetProperty = (String) analysis.getValue().get("target_property");

        if (analysis.getValue().get("percentile") != null) {
            return new SubAnalysis(label, analysisType, targetProperty,
                    Percentile.createCoerced(Double.valueOf((String) analysis.getValue().get("percentile")))
            );
        }
        return new SubAnalysis(label, analysisType, targetProperty);
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

    public List<SubAnalysis> getAnalyses() {
        return analyses;
    }

    public void setAnalyses(SubAnalysis... analyses) {
        this.analyses = Arrays.asList(analyses);
    }

    public void setAnalyses(List<SubAnalysis> analyses) {
        this.analyses = analyses;
    }

    public String getTargetProperty() {
        return targetProperty;
    }

    public void setTargetProperty(String targetProperty) {
        this.targetProperty = targetProperty;
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

    public void setGroupBy(String... groupBy) {
        this.groupBy = Arrays.asList(groupBy);
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
    }

    Map<String, Object> asMap() {
        Map<String, Object> result = new HashMap<String, Object>();
        if (analysisType != null) {
            result.put(ANALYSIS_TYPE, analysisType);
        }
        if (targetProperty != null) {
            result.put(TARGET_PROPERTY, targetProperty);
        }
        if (eventCollection != null) {
            result.put(EVENT_COLLECTION, eventCollection);
        }
        if (timeframe != null) {
            result.put(TIMEFRAME, timeframe);
        }
        if (timezone != null) {
            result.put(TIMEZONE, timezone);
        }
        if (interval != null) {
            result.put(INTERVAL, interval);
        }
        if (groupBy != null) {
            result.put(GROUP_BY, groupBy);
        }
        if (analyses != null) {
            Map<String, Map<String, Object>> analysesMap = new HashMap<String, Map<String, Object>>();
            for (SubAnalysis analysis : analyses) {
                analysesMap.put(analysis.getLabel(), analysis.constructParameterRequestArgs());
            }
            result.put(KeenQueryConstants.ANALYSES, analysesMap);
        }
        if (filters != null) {
            List<Map<String, ?>> filtersList = new LinkedList<Map<String, ?>>();
            for (Filter filter : filters) {
                filtersList.add(filter.constructParameterRequestArgs());
            }
            result.put(FILTERS, filtersList);
        }
        return result;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
    }
}
