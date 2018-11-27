package io.keen.client.java;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
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

    public String getProjectId() {
        return projectId;
    }

    public String getAnalysisType() {
        return analysisType;
    }

    public List<SubAnalysis> getAnalyses() {
        return analyses;
    }

    public String getTargetProperty() {
        return targetProperty;
    }

    public String getEventCollection() {
        return eventCollection;
    }

    public String getTimezone() {
        return timezone;
    }

    public String getInterval() {
        return interval;
    }

    public String getTimeframe() {
        return timeframe;
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    public static final class DatasetQueryBuilder {
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

        private DatasetQueryBuilder() {
        }

        public static DatasetQueryBuilder aDatasetQuery() {
            return new DatasetQueryBuilder();
        }

        public DatasetQueryBuilder withProjectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public DatasetQueryBuilder withAnalysisType(String analysisType) {
            this.analysisType = analysisType;
            return this;
        }

        public DatasetQueryBuilder withAnalyses(List<SubAnalysis> analyses) {
            this.analyses = analyses;
            return this;
        }

        public DatasetQueryBuilder withTargetProperty(String targetProperty) {
            this.targetProperty = targetProperty;
            return this;
        }

        public DatasetQueryBuilder withEventCollection(String eventCollection) {
            this.eventCollection = eventCollection;
            return this;
        }

        public DatasetQueryBuilder withTimezone(String timezone) {
            this.timezone = timezone;
            return this;
        }

        public DatasetQueryBuilder withInterval(String interval) {
            this.interval = interval;
            return this;
        }

        public DatasetQueryBuilder withTimeframe(String timeframe) {
            this.timeframe = timeframe;
            return this;
        }

        public DatasetQueryBuilder withGroupBy(List<String> groupBy) {
            this.groupBy = groupBy;
            return this;
        }

        public DatasetQueryBuilder withFilters(List<Filter> filters) {
            this.filters = filters;
            return this;
        }

        public DatasetQuery build() {
            DatasetQuery datasetQuery = new DatasetQuery();
            datasetQuery.targetProperty = this.targetProperty;
            datasetQuery.timezone = this.timezone;
            datasetQuery.analysisType = this.analysisType;
            datasetQuery.eventCollection = this.eventCollection;
            datasetQuery.filters = this.filters;
            datasetQuery.interval = this.interval;
            datasetQuery.groupBy = this.groupBy;
            datasetQuery.projectId = this.projectId;
            datasetQuery.timeframe = this.timeframe;
            datasetQuery.analyses = this.analyses;
            return datasetQuery;
        }
    }
}
