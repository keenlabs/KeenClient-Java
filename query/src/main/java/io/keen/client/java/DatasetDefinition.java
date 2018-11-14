package io.keen.client.java;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DatasetDefinition {

    private String datasetName;

    private String displayName;

    private String projectId;

    private String organizationId;

    private String lastScheduledDate;

    private String latestSubtimeframeAvailable;

    private String millisecondsBehind;

    private List<String> indexBy;

    private DatasetQuery query;

    @SuppressWarnings("unchecked")
    static DatasetDefinition fromMap(Map<String, ?> properties) {
        DatasetDefinition definition = new DatasetDefinition();
        definition.datasetName = (String) properties.get("dataset_name");
        definition.displayName = (String) properties.get("display_name");
        definition.projectId = (String) properties.get("project_id");
        definition.organizationId = (String) properties.get("organization_id");
        definition.lastScheduledDate = (String) properties.get("last_scheduled_date");
        definition.latestSubtimeframeAvailable = (String) properties.get("latest_subtimeframe_available");
        definition.millisecondsBehind = String.valueOf(properties.get("milliseconds_behind"));
        definition.indexBy = (List<String>) properties.get("index_by");
        definition.query = DatasetQuery.fromMap((Map<String, Object>) properties.get("query"));
        return definition;
    }

    @SuppressWarnings("unchecked")
    static List<DatasetDefinition> definitionsFromMap(Map<String, ?> properties) {
        if (properties.get("datasets") == null) {
            return Collections.emptyList();
        }
        List<Map<String, ?>> datasets = (List<Map<String, ?>>) properties.get("datasets");
        List<DatasetDefinition> definitions = new LinkedList<DatasetDefinition>();

        for (Map<String, ?> dataset : datasets) {
            definitions.add(fromMap(dataset));
        }

        return definitions;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getOrganizationId() {
        return organizationId;
    }

    public void setOrganizationId(String organizationId) {
        this.organizationId = organizationId;
    }

    public String getLastScheduledDate() {
        return lastScheduledDate;
    }

    public void setLastScheduledDate(String lastScheduledDate) {
        this.lastScheduledDate = lastScheduledDate;
    }

    public String getLatestSubtimeframeAvailable() {
        return latestSubtimeframeAvailable;
    }

    public void setLatestSubtimeframeAvailable(String latestSubtimeframeAvailable) {
        this.latestSubtimeframeAvailable = latestSubtimeframeAvailable;
    }

    public String getMillisecondsBehind() {
        return millisecondsBehind;
    }

    public void setMillisecondsBehind(String millisecondsBehind) {
        this.millisecondsBehind = millisecondsBehind;
    }

    public List<String> getIndexBy() {
        return indexBy;
    }

    public void setIndexBy(List<String> indexBy) {
        this.indexBy = indexBy;
    }

    public DatasetQuery getQuery() {
        return query;
    }

    public void setQuery(DatasetQuery query) {
        this.query = query;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
    }
}
