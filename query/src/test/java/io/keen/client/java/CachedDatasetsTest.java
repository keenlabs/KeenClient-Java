package io.keen.client.java;

import io.keen.client.java.result.*;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@SuppressWarnings({"unchecked", "unused"})
@RunWith(JUnitParamsRunner.class)
public class CachedDatasetsTest extends KeenQueryTestBase {

    private static final String NULL = null;

    private CachedDatasets cachedDatasets;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Override
    public void setup() throws IOException {
        super.setup();
        cachedDatasets = queryClient.getCachedDatasetsClient();
    }

    @Test
    @Parameters
    public void shouldFailToCreateCachedDataset(String datasetName, String displayName, DatasetQuery query, Collection<String> indexBy, String expectedMessage) throws IOException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(expectedMessage);

        cachedDatasets.create(datasetName, displayName, query, indexBy);
    }

    private Object parametersForShouldFailToCreateCachedDataset() {
        return new Object[][]{
                {null, "display name", mock(DatasetQuery.class), mock(Collection.class), "Dataset name cannot be blank"},
                {"", "display name", mock(DatasetQuery.class), mock(Collection.class), "Dataset name cannot be blank"},
                {" ", "display name", mock(DatasetQuery.class), mock(Collection.class), "Dataset name cannot be blank"},

                {"dataset-name", null, mock(DatasetQuery.class), mock(Collection.class), "Display name cannot be blank"},
                {"dataset-name", "", mock(DatasetQuery.class), mock(Collection.class), "Display name cannot be blank"},
                {"dataset-name", " ", mock(DatasetQuery.class), mock(Collection.class), "Display name cannot be blank"},

                {"dataset-name", "display name", null, mock(Collection.class), "Dataset query is required"},

                {"dataset-name", "display name", mock(DatasetQuery.class), null, "At least one index property is required"},
                {"dataset-name", "display name", mock(DatasetQuery.class), Collections.<String>emptyList(), "At least one index property is required"},
        };
    }

    @Test
    public void shouldCreateCachedDataset() throws IOException {
        setMockResponse(201, "{\n" +
                "  \"dataset_name\": \"testing-things\",\n" +
                "  \"display_name\": \"TESTING THINGS\",\n" +
                "  \"query\": {\n" +
                "    \"project_id\": \"project-id\",\n" +
                "    \"analysis_type\": \"multi_analysis\",\n" +
                "    \"event_collection\": \"add_project_member\",\n" +
                "    \"filters\": [\n" +
                "      {\n" +
                "        \"property_name\": \"organization.id\",\n" +
                "        \"operator\": \"eq\",\n" +
                "        \"property_value\": \"specific-org-id\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"timeframe\": \"this_6_months\",\n" +
                "    \"timezone\": \"UTC\",\n" +
                "    \"interval\": \"monthly\",\n" +
                "    \"group_by\": [\n" +
                "      \"user.uuid\"\n" +
                "    ],\n" +
                "    \"analyses\": {\n" +
                "      \"select unique\": {\n" +
                "        \"analysis_type\": \"select_unique\",\n" +
                "        \"target_property\": \"user.email\",\n" +
                "        \"percentile\": null\n" +
                "      },\n" +
                "      \"total count\": {\n" +
                "        \"analysis_type\": \"count\",\n" +
                "        \"target_property\": null,\n" +
                "        \"percentile\": null\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"index_by\": [\n" +
                "    \"organization.id\",\n" +
                "    \"project.has_saved_queries\"\n" +
                "  ],\n" +
                "  \"last_scheduled_date\": null,\n" +
                "  \"latest_subtimeframe_available\": null,\n" +
                "  \"milliseconds_behind\": 0\n" +
                "}");

        DatasetQuery datasetQuery = DatasetQuery.DatasetQueryBuilder
                .aDatasetQuery()
                .withAnalysisType("multi_analysis")
                .withAnalyses(asList(new SubAnalysis("total count", QueryType.COUNT), new SubAnalysis("select unique", QueryType.SELECT_UNIQUE, "user.email")))
                .withEventCollection("add_project_member")
                .withFilters(singletonList(new Filter("organization.id", FilterOperator.EQUAL_TO, "specific-org-id")))
                .withGroupBy(singletonList("user.uuid"))
                .withTimeframe("this_6_months")
                .withTimezone("UTC")
                .withInterval("monthly")
                .build();

        List<String> indexByValues = Arrays.asList("organization.id", "project.has_saved_queries");

        DatasetDefinition datasetDefinition = cachedDatasets.create("testing-things", "testing things", datasetQuery, indexByValues);

        assertEquals("testing-things", datasetDefinition.getDatasetName());
        assertEquals("TESTING THINGS", datasetDefinition.getDisplayName());
        assertEquals(asList("organization.id", "project.has_saved_queries"), datasetDefinition.getIndexBy());
        assertNull(datasetDefinition.getLastScheduledDate());
        assertNull(datasetDefinition.getLatestSubtimeframeAvailable());
        assertEquals("0", datasetDefinition.getMillisecondsBehind());

        DatasetQuery query = datasetDefinition.getQuery();
        assertEquals("project-id", query.getProjectId());
        assertEquals("multi_analysis", query.getAnalysisType());
        assertEquals("add_project_member", query.getEventCollection());
        assertEquals("this_6_months", query.getTimeframe());
        assertEquals("UTC", query.getTimezone());
        assertEquals(singletonList("user.uuid"), query.getGroupBy());
        assertEquals(1, query.getFilters().size());
        assertEquals(2, query.getAnalyses().size());

        Map<String, Object> filter = query.getFilters().get(0).constructParameterRequestArgs();
        assertEquals("organization.id", filter.get("property_name"));
        assertEquals("eq", filter.get("operator"));
        assertEquals("specific-org-id", filter.get("property_value"));

        SubAnalysis selectUniqueAnalysis = query.getAnalyses().get(0);
        SubAnalysis countAnalysis = query.getAnalyses().get(1);


        assertEquals("select unique", selectUniqueAnalysis.getLabel());
        assertEquals("select_unique", selectUniqueAnalysis.constructParameterRequestArgs().get("analysis_type"));
        assertEquals("user.email", selectUniqueAnalysis.constructParameterRequestArgs().get("target_property"));
        assertNull(selectUniqueAnalysis.constructParameterRequestArgs().get("percentile"));

        assertEquals("total count", countAnalysis.getLabel());
        assertEquals("count", countAnalysis.constructParameterRequestArgs().get("analysis_type"));
        assertNull(countAnalysis.constructParameterRequestArgs().get("target_property"));
        assertNull(countAnalysis.constructParameterRequestArgs().get("percentile"));
    }

    @Test
    @Parameters
    public void shouldFailToCachedDatasetDefinition(String datasetName) throws IOException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Dataset name cannot be blank");

        cachedDatasets.getDefinition(datasetName);
    }

    private Object parametersForShouldFailToCachedDatasetDefinition() {
        return new Object[]{
                null, "", " "
        };
    }

    @Test
    public void shouldGetCachedDatasetDefinition() throws IOException {
        setMockResponse(200, "{\n" +
                "  \"dataset_name\": \"testing-things\",\n" +
                "  \"display_name\": \"TESTING THINGS\",\n" +
                "  \"query\": {\n" +
                "    \"project_id\": \"project-id\",\n" +
                "    \"analysis_type\": \"multi_analysis\",\n" +
                "    \"event_collection\": \"add_project_member\",\n" +
                "    \"filters\": [\n" +
                "      {\n" +
                "        \"property_name\": \"organization.id\",\n" +
                "        \"operator\": \"eq\",\n" +
                "        \"property_value\": \"specific-org-id\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"timeframe\": \"this_6_months\",\n" +
                "    \"timezone\": \"UTC\",\n" +
                "    \"interval\": \"monthly\",\n" +
                "    \"group_by\": [\n" +
                "      \"user.uuid\"\n" +
                "    ],\n" +
                "    \"analyses\": {\n" +
                "      \"select unique\": {\n" +
                "        \"analysis_type\": \"select_unique\",\n" +
                "        \"target_property\": \"user.email\",\n" +
                "        \"percentile\": null\n" +
                "      },\n" +
                "      \"total count\": {\n" +
                "        \"analysis_type\": \"count\",\n" +
                "        \"target_property\": null,\n" +
                "        \"percentile\": null\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"index_by\": [\n" +
                "    \"organization.id\",\n" +
                "    \"project.has_saved_queries\"\n" +
                "  ],\n" +
                "  \"last_scheduled_date\": \"2018-11-15T15:22:05.192Z\",\n" +
                "  \"latest_subtimeframe_available\": \"2018-12-01T00:00:00.000Z\",\n" +
                "  \"milliseconds_behind\": 12341234\n" +
                "}");

        DatasetDefinition datasetDefinition = cachedDatasets.getDefinition("testing-things");

        assertEquals("testing-things", datasetDefinition.getDatasetName());
        assertEquals("TESTING THINGS", datasetDefinition.getDisplayName());
        assertEquals(asList("organization.id", "project.has_saved_queries"), datasetDefinition.getIndexBy());
        assertEquals("2018-11-15T15:22:05.192Z", datasetDefinition.getLastScheduledDate());
        assertEquals("2018-12-01T00:00:00.000Z", datasetDefinition.getLatestSubtimeframeAvailable());
        assertEquals("12341234", datasetDefinition.getMillisecondsBehind());

        DatasetQuery query = datasetDefinition.getQuery();
        assertEquals("project-id", query.getProjectId());
        assertEquals("multi_analysis", query.getAnalysisType());
        assertEquals("add_project_member", query.getEventCollection());
        assertEquals("this_6_months", query.getTimeframe());
        assertEquals("UTC", query.getTimezone());
        assertEquals(singletonList("user.uuid"), query.getGroupBy());
        assertEquals(1, query.getFilters().size());
        assertEquals(2, query.getAnalyses().size());

        Map<String, Object> filter = query.getFilters().get(0).constructParameterRequestArgs();
        assertEquals("organization.id", filter.get("property_name"));
        assertEquals("eq", filter.get("operator"));
        assertEquals("specific-org-id", filter.get("property_value"));

        SubAnalysis selectUniqueAnalysis = query.getAnalyses().get(0);
        SubAnalysis countAnalysis = query.getAnalyses().get(1);


        assertEquals("select unique", selectUniqueAnalysis.getLabel());
        assertEquals("select_unique", selectUniqueAnalysis.constructParameterRequestArgs().get("analysis_type"));
        assertEquals("user.email", selectUniqueAnalysis.constructParameterRequestArgs().get("target_property"));
        assertNull(selectUniqueAnalysis.constructParameterRequestArgs().get("percentile"));

        assertEquals("total count", countAnalysis.getLabel());
        assertEquals("count", countAnalysis.constructParameterRequestArgs().get("analysis_type"));
        assertNull(countAnalysis.constructParameterRequestArgs().get("target_property"));
        assertNull(countAnalysis.constructParameterRequestArgs().get("percentile"));
    }

    @Test
    @Parameters
    public void shouldFailToGetResults(DatasetDefinition datasetDefinition, Map<String, ?> indexByValues, Timeframe timeframe, String exceptionMessage) throws IOException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(exceptionMessage);

        cachedDatasets.getResults(datasetDefinition, indexByValues, timeframe);
    }

    private Object parametersForShouldFailToGetResults() {
        DatasetDefinition datasetDefinition = DatasetDefinition.DatasetDefinitionBuilder
                .aDatasetDefinition()
                .withDatasetName("testing-things")
                .withDisplayName("TESTING THINGS")
                .withIndexBy(asList("organization.id", "project.has_saved_queries"))
                .withQuery(mock(DatasetQuery.class))
                .build();

        HashMap<String, Object> indexByValues = new HashMap<String, Object>() {{
            put("organization.id", "specific-org-id");
            put("project.has_saved_queries", true);
        }};
        return new Object[][]{
                {null, indexByValues, new RelativeTimeframe("previous_3_months"), "Dataset definition is required"},

                {datasetDefinition, null, new RelativeTimeframe("previous_3_months"),
                        "Values for all index_by properties are required: [organization.id, project.has_saved_queries]"},
                {datasetDefinition, Collections.<String, Object>emptyMap(), new RelativeTimeframe("previous_3_months"),
                        "Values for all index_by properties are required: [organization.id, project.has_saved_queries]"},
                {datasetDefinition, Collections.<String, Object>singletonMap("project.has_saved_queries", true), new RelativeTimeframe("previous_3_months"),
                        "Values for the following index_by properties must be present: [organization.id, project.has_saved_queries]. Found for: [project.has_saved_queries]"},
                {datasetDefinition, new HashMap<String, Object>(indexByValues) {{
                    put("additional.property", false);
                }}, new RelativeTimeframe("previous_3_months"),
                        "Values for the following index_by properties must be present: [organization.id, project.has_saved_queries]. Found for: [additional.property, organization.id, project.has_saved_queries]"},

                {datasetDefinition, indexByValues, null, "Timeframe is required"},
        };
    }

    @Test
    public void shouldGetResults() throws IOException {
        setMockResponse(200, "{\n" +
                "    \"result\": [\n" +
                "        {\n" +
                "            \"timeframe\": {\n" +
                "                \"start\": \"2018-10-01T00:00:00.000Z\",\n" +
                "                \"end\": \"2018-11-01T00:00:00.000Z\"\n" +
                "            },\n" +
                "            \"value\": [\n" +
                "                {\n" +
                "                    \"user.uuid\": \"a4dfaea788e41eccef2cfff8d05a60bd\",\n" +
                "                    \"select unique\": [\n" +
                "                        \"user1@example.com\",\n" +
                "                        \"user2@example.com\"\n" +
                "                    ],\n" +
                "                    \"total count\": 11\n" +
                "                }\n" +
                "            ]\n" +
                "        },\n" +
                "        {\n" +
                "            \"timeframe\": {\n" +
                "                \"start\": \"2018-11-01T00:00:00.000Z\",\n" +
                "                \"end\": \"2018-12-01T00:00:00.000Z\"\n" +
                "            },\n" +
                "            \"value\": [\n" +
                "                {\n" +
                "                    \"user.uuid\": \"a4dfaea788e41eccef2cfff8d05a60bd\",\n" +
                "                    \"select unique\": [],\n" +
                "                    \"total count\": 0\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ],\n" +
                "    \"metadata\": {\n" +
                "        \"dataset\": {\n" +
                "            \"dataset_name\": \"testing-things\",\n" +
                "            \"display_name\": \"TESTING THINGS\",\n" +
                "            \"query\": {\n" +
                "                \"project_id\": \"project-id\",\n" +
                "                \"analysis_type\": \"multi_analysis\",\n" +
                "                \"event_collection\": \"add_project_member\",\n" +
                "                \"filters\": [\n" +
                "                    {\n" +
                "                        \"property_name\": \"organization.id\",\n" +
                "                        \"operator\": \"eq\",\n" +
                "                        \"property_value\": \"specific-org-id\"\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"timeframe\": \"this_6_months\",\n" +
                "                \"timezone\": \"UTC\",\n" +
                "                \"interval\": \"monthly\",\n" +
                "                \"group_by\": [\n" +
                "                    \"user.uuid\"\n" +
                "                ],\n" +
                "                \"analyses\": {\n" +
                "                    \"select unique\": {\n" +
                "                        \"analysis_type\": \"select_unique\",\n" +
                "                        \"target_property\": \"user.email\",\n" +
                "                        \"percentile\": null\n" +
                "                    },\n" +
                "                    \"total count\": {\n" +
                "                        \"analysis_type\": \"count\",\n" +
                "                        \"target_property\": null,\n" +
                "                        \"percentile\": null\n" +
                "                    }\n" +
                "                }\n" +
                "            },\n" +
                "            \"index_by\": [\n" +
                "                \"organization.id\",\n" +
                "                \"project.has_saved_queries\"\n" +
                "            ],\n" +
                "            \"last_scheduled_date\": \"2018-11-15T15:22:05.192Z\",\n" +
                "            \"latest_subtimeframe_available\": \"2018-12-01T00:00:00.000Z\",\n" +
                "            \"milliseconds_behind\": 0\n" +
                "        },\n" +
                "        \"request\": {\n" +
                "            \"index_by\": {\n" +
                "                \"organization.id\": \"specific-org-id\",\n" +
                "                \"project.has_saved_queries\": true\n" +
                "            },\n" +
                "            \"timeframe\": \"this_2_months\"\n" +
                "        }\n" +
                "    }\n" +
                "}");

        DatasetQuery query = DatasetQuery.DatasetQueryBuilder
                .aDatasetQuery()
                .withAnalysisType("multi_analysis")
                .withAnalyses(asList(new SubAnalysis("total count", QueryType.COUNT), new SubAnalysis("select unique", QueryType.SELECT_UNIQUE, "user.email")))
                .withEventCollection("add_project_member")
                .withFilters(singletonList(new Filter("organization.id", FilterOperator.EQUAL_TO, "specific-org-id")))
                .withGroupBy(singletonList("user.uuid"))
                .withTimeframe("this_6_months")
                .withTimezone("UTC")
                .withInterval("monthly")
                .build();

        DatasetDefinition datasetDefinition = DatasetDefinition.DatasetDefinitionBuilder
                .aDatasetDefinition()
                .withDatasetName("testing-things")
                .withDisplayName("TESTING THINGS")
                .withIndexBy(asList("organization.id", "project.has_saved_queries"))
                .withQuery(query)
                .build();

        HashMap<String, Object> indexByValues = new HashMap<String, Object>() {{
            put("organization.id", "specific-org-id");
            put("project.has_saved_queries", true);
        }};

        List<IntervalResultValue> results = cachedDatasets.getResults(datasetDefinition, indexByValues, new RelativeTimeframe("this_3_months"));

        assertEquals(2, results.size());

        IntervalResultValue october = results.get(0);

        assertEquals("2018-10-01T00:00:00.000Z", october.getTimeframe().getStart());
        assertEquals("2018-11-01T00:00:00.000Z", october.getTimeframe().getEnd());
        assertTrue(october.getResult().isGroupResult());

        QueryResult octoberQueryResult = october.getResult().getGroupResults().get(new Group(Collections.singletonMap("user.uuid", "a4dfaea788e41eccef2cfff8d05a60bd")));

        assertTrue(octoberQueryResult instanceof MultiAnalysisResult);

        assertEquals(new ListResult(asList(new StringResult("user1@example.com"), new StringResult("user2@example.com"))), ((MultiAnalysisResult) octoberQueryResult).getResultFor("select unique"));
        assertEquals(new LongResult(11L), ((MultiAnalysisResult) octoberQueryResult).getResultFor("total count"));


        IntervalResultValue november = results.get(1);

        assertEquals("2018-11-01T00:00:00.000Z", november.getTimeframe().getStart());
        assertEquals("2018-12-01T00:00:00.000Z", november.getTimeframe().getEnd());
        assertTrue(november.getResult().isGroupResult());

        QueryResult novemberQueryResult = november.getResult().getGroupResults().get(new Group(Collections.singletonMap("user.uuid", "a4dfaea788e41eccef2cfff8d05a60bd")));

        assertTrue(novemberQueryResult instanceof MultiAnalysisResult);

        assertEquals(new ListResult(Collections.<QueryResult>emptyList()), ((MultiAnalysisResult) novemberQueryResult).getResultFor("select unique"));
        assertEquals(new LongResult(0L), ((MultiAnalysisResult) novemberQueryResult).getResultFor("total count"));

        // and the second invocation in order to make sure that equals() & hashCode() are implemented properly

        List<IntervalResultValue> results2 = cachedDatasets.getResults(datasetDefinition, indexByValues, new RelativeTimeframe("this_3_months"));
        
        assertEquals(results, results2);
    }

    @Test
    public void shouldGetCachedDatasetsByProject() throws IOException {
        setMockResponse(200, "{\n" +
                "    \"datasets\": [\n" +
                "        {\n" +
                "            \"dataset_name\": \"dataset-1\",\n" +
                "            \"display_name\": \"dataset-1\",\n" +
                "            \"query\": {\n" +
                "                \"project_id\": \"project-id\",\n" +
                "                \"analysis_type\": \"multi_analysis\",\n" +
                "                \"event_collection\": \"analysis_api_call\",\n" +
                "                \"filters\": [\n" +
                "                    {\n" +
                "                        \"property_name\": \"response.status_code\",\n" +
                "                        \"operator\": \"gt\",\n" +
                "                        \"property_value\": 200\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"timeframe\": \"this_53_weeks\",\n" +
                "                \"timezone\": \"UTC\",\n" +
                "                \"interval\": \"weekly\",\n" +
                "                \"group_by\": [\n" +
                "                    \"response.status_code\",\n" +
                "                    \"analysis_type\"\n" +
                "                ],\n" +
                "                \"analyses\": {\n" +
                "                    \"unique-org\": {\n" +
                "                        \"analysis_type\": \"count_unique\",\n" +
                "                        \"target_property\": \"project.organization.name\",\n" +
                "                        \"percentile\": null\n" +
                "                    }\n" +
                "                }\n" +
                "            },\n" +
                "            \"index_by\": [\n" +
                "                \"project.organization.id\"\n" +
                "            ],\n" +
                "            \"last_scheduled_date\": \"2018-11-15T16:24:06.711Z\",\n" +
                "            \"latest_subtimeframe_available\": \"2018-11-18T00:00:00.000Z\",\n" +
                "            \"milliseconds_behind\": 0\n" +
                "        },\n" +
                "        {\n" +
                "            \"dataset_name\": \"dataset-2\",\n" +
                "            \"display_name\": \"DATASET 2\",\n" +
                "            \"query\": {\n" +
                "                \"project_id\": \"project-id\",\n" +
                "                \"analysis_type\": \"multi_analysis\",\n" +
                "                \"event_collection\": \"analysis_api_call\",\n" +
                "                \"filters\": [],\n" +
                "                \"timeframe\": \"this_3_months\",\n" +
                "                \"timezone\": null,\n" +
                "                \"interval\": \"monthly\",\n" +
                "                \"group_by\": [],\n" +
                "                \"analyses\": {\n" +
                "                    \"events_used\": {\n" +
                "                        \"analysis_type\": \"sum\",\n" +
                "                        \"target_property\": \"metadata.eventsUsed\",\n" +
                "                        \"percentile\": null\n" +
                "                    },\n" +
                "                    \"query_count\": {\n" +
                "                        \"analysis_type\": \"count\",\n" +
                "                        \"target_property\": null,\n" +
                "                        \"percentile\": null\n" +
                "                    }\n" +
                "                }\n" +
                "            },\n" +
                "            \"index_by\": [\n" +
                "                \"project.organization.id\"\n" +
                "            ],\n" +
                "            \"last_scheduled_date\": \"2018-11-15T15:46:07.203Z\",\n" +
                "            \"latest_subtimeframe_available\": \"2018-12-01T00:00:00.000Z\",\n" +
                "            \"milliseconds_behind\": 0\n" +
                "        }\n" +
                "    ],\n" +
                "    \"next_page_url\": \"https://api.keen.io/3.0/projects/project-id/datasets?limit=2&after_name=dataset-2\",\n" +
                "    \"count\": 43\n" +
                "}");

        List<DatasetDefinition> definitions = cachedDatasets.getDefinitionsByProject(2, "some-name");

        assertEquals(2, definitions.size());
        assertEquals("dataset-1", definitions.get(0).getDatasetName());
        assertEquals("dataset-2", definitions.get(1).getDatasetName());

        // also checking default behaviour
        assertEquals(definitions, cachedDatasets.getDefinitionsByProject());
    }

    @Test
    @Parameters
    public void shouldFailToDeleteDatasetDefinition(String datasetName) throws IOException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Dataset name cannot be blank");

        cachedDatasets.delete(datasetName);
    }

    private Object parametersForShouldFailToDeleteDatasetDefinition() {
        return new Object[]{
                null, "", " "
        };
    }

    @Test
    public void shouldDeleteCachedDataset() throws IOException {
        setMockResponse(204, "");

        boolean result = cachedDatasets.delete("testing-things");

        assertTrue(result);
    }
}
