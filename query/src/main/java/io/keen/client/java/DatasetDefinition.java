package io.keen.client.java;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
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

    public String getDisplayName() {
        return displayName;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getOrganizationId() {
        return organizationId;
    }

    public String getLastScheduledDate() {
        return lastScheduledDate;
    }

    public String getLatestSubtimeframeAvailable() {
        return latestSubtimeframeAvailable;
    }

    public String getMillisecondsBehind() {
        return millisecondsBehind;
    }

    public List<String> getIndexBy() {
        return indexBy;
    }

    public DatasetQuery getQuery() {
        return query;
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

    public static final class DatasetDefinitionBuilder {
        private String datasetName;
        private String displayName;
        private String projectId;
        private String organizationId;
        private String lastScheduledDate;
        private String latestSubtimeframeAvailable;
        private String millisecondsBehind;
        private List<String> indexBy;
        private DatasetQuery query;

        private DatasetDefinitionBuilder() {
        }

        public static DatasetDefinitionBuilder aDatasetDefinition() {
            return new DatasetDefinitionBuilder();
        }

        public DatasetDefinitionBuilder withDatasetName(String datasetName) {
            this.datasetName = datasetName;
            return this;
        }

        public DatasetDefinitionBuilder withDisplayName(String displayName) {
            this.displayName = displayName;
            return this;
        }

        public DatasetDefinitionBuilder withProjectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public DatasetDefinitionBuilder withOrganizationId(String organizationId) {
            this.organizationId = organizationId;
            return this;
        }

        public DatasetDefinitionBuilder withLastScheduledDate(String lastScheduledDate) {
            this.lastScheduledDate = lastScheduledDate;
            return this;
        }

        public DatasetDefinitionBuilder withLatestSubtimeframeAvailable(String latestSubtimeframeAvailable) {
            this.latestSubtimeframeAvailable = latestSubtimeframeAvailable;
            return this;
        }

        public DatasetDefinitionBuilder withMillisecondsBehind(String millisecondsBehind) {
            this.millisecondsBehind = millisecondsBehind;
            return this;
        }

        public DatasetDefinitionBuilder withIndexBy(List<String> indexBy) {
            this.indexBy = indexBy;
            return this;
        }

        public DatasetDefinitionBuilder withQuery(DatasetQuery query) {
            this.query = query;
            return this;
        }

        public DatasetDefinition build() {
            DatasetDefinition datasetDefinition = new DatasetDefinition();
            datasetDefinition.datasetName = this.datasetName;
            datasetDefinition.projectId = this.projectId;
            datasetDefinition.organizationId = this.organizationId;
            datasetDefinition.millisecondsBehind = this.millisecondsBehind;
            datasetDefinition.lastScheduledDate = this.lastScheduledDate;
            datasetDefinition.indexBy = this.indexBy;
            datasetDefinition.query = this.query;
            datasetDefinition.displayName = this.displayName;
            datasetDefinition.latestSubtimeframeAvailable = this.latestSubtimeframeAvailable;
            return datasetDefinition;
        }
    }
}
