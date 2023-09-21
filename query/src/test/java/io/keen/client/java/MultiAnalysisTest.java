package io.keen.client.java;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Test;

import java.time.Instant;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.keen.client.java.result.Group;
import io.keen.client.java.result.IntervalResultValue;
import io.keen.client.java.result.MultiAnalysisResult;
import io.keen.client.java.result.QueryResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test the MultiAnalysis query functionality.
 *
 * @author masojus
 */
public class MultiAnalysisTest extends KeenQueryTestBase {
    @Test
    public void testMultiAnalysis_Simple() throws Exception {
        // Response doesn't really matter here, but this is what it'd look like.
        setMockResponse(200, "{" +
                    "\"result\": {" +
                        "\"plain_old_count\": 24" +
                    "}" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .build();

        ObjectNode requestNode = getMultiAnalysisRequestNode(multiAnalysisParams);

        // Should have 'event_collection', 'analyses' and 'timeframe' top-level keys.
        assertEquals(3, requestNode.size());

        ObjectNode analysesNode = (ObjectNode)requestNode.get(KeenQueryConstants.ANALYSES);
        assertEquals(1, analysesNode.size());
        ObjectNode countNode = (ObjectNode)analysesNode.get("plain_old_count");
        assertEquals(1, countNode.size());
        assertNull("There should be no 'target_property' for 'count' analysis.",
                countNode.get(KeenQueryConstants.TARGET_PROPERTY));
        assertEquals(KeenQueryConstants.COUNT,
                countNode.get(KeenQueryConstants.ANALYSIS_TYPE).asText());
    }

    @Test
    public void testMultiAnalysis_Percentile() throws Exception {
        // Response doesn't really matter here, but this is what it'd look like.
        setMockResponse(200, "{" +
                    "\"result\": {" +
                        "\"the_average\": 53.768923," +
                        "\"categories\": 3," +
                        "\"maybe_percentile\": 27," +
                        "\"plain_old_count\": 24," +
                        "\"the_total\": 1235" +
                    "}" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis(
                        "the_average",
                        QueryType.AVERAGE,
                        TEST_TARGET_PROPERTY))
                .withSubAnalysis(new SubAnalysis("categories", QueryType.COUNT_UNIQUE, "category"))
                .withSubAnalysis(new SubAnalysis("maybe_percentile",
                        QueryType.PERCENTILE,
                        TEST_TARGET_PROPERTY,
                        Percentile.createStrict(30.0)))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withSubAnalysis(new SubAnalysis("the_total", QueryType.SUM, TEST_TARGET_PROPERTY))
                .build();

        ObjectNode requestNode = getMultiAnalysisRequestNode(multiAnalysisParams);

        // Should have 'event_collection', 'analyses' and 'timeframe' top-level keys
        assertEquals(3, requestNode.size());

        ObjectNode analysesNode = (ObjectNode)requestNode.get(KeenQueryConstants.ANALYSES);
        assertEquals(5, analysesNode.size());

        ObjectNode averageNode = (ObjectNode)analysesNode.get("the_average");
        assertEquals(2, averageNode.size());
        assertEquals(KeenQueryConstants.AVERAGE,
                averageNode.get(KeenQueryConstants.ANALYSIS_TYPE).asText());
        assertEquals(TEST_TARGET_PROPERTY,
                averageNode.get(KeenQueryConstants.TARGET_PROPERTY).asText());

        ObjectNode percentileNode = (ObjectNode)analysesNode.get("maybe_percentile");
        assertEquals(3, percentileNode.size());
        assertEquals(KeenQueryConstants.PERCENTILE,
                percentileNode.get(KeenQueryConstants.ANALYSIS_TYPE).asText());
        assertEquals(TEST_TARGET_PROPERTY,
                percentileNode.get(KeenQueryConstants.TARGET_PROPERTY).asText());
        assertEquals(30.0,
                percentileNode.get(KeenQueryConstants.PERCENTILE).asDouble(), DOUBLE_CMP_DELTA);
    }

    @Test
    public void testMultiAnalysis_GroupBy() throws Exception {
        // Response doesn't really matter here, but this is what it'd look like.
        setMockResponse(200, "{" +
                    "\"result\": [{" +
                        "\"category\": \"first\"," +
                        "\"plain_old_count\": 17" +
                    "}, {" +
                        "\"category\": \"second\"," +
                        "\"plain_old_count\": 31" +
                    "}]" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withGroupBy("category")
                .build();

        ObjectNode requestNode = getMultiAnalysisRequestNode(multiAnalysisParams);

        // Should have 'event_collection', 'analyses', 'timeframe' and 'group_by' top-level keys
        assertEquals(4, requestNode.size());

        // Make sure the 'count' sub-analysis is still there even though we specified 'group_by'...
        ObjectNode analysesNode = (ObjectNode)requestNode.get(KeenQueryConstants.ANALYSES);
        assertEquals(1, analysesNode.size());
        ObjectNode countNode = (ObjectNode)analysesNode.get("plain_old_count");
        assertEquals(1, countNode.size());
        assertNull("There should be no 'target_property' for 'count' analysis.",
                countNode.get(KeenQueryConstants.TARGET_PROPERTY));
        assertEquals(KeenQueryConstants.COUNT,
                countNode.get(KeenQueryConstants.ANALYSIS_TYPE).asText());

        // ...but also make sure the 'group_by' is there now.
        // We always JSONify a collection for groupBy, so this should be an array.
        ArrayNode groupByNode = (ArrayNode)requestNode.get(KeenQueryConstants.GROUP_BY);
        assertEquals(1, groupByNode.size()); // Only one 'group_by' parameter here
        assertEquals("category", groupByNode.get(0).asText());
    }

    @Test
    public void testMultiAnalysis_MultipleGroupBy() throws Exception {
        // Response doesn't really matter here, but this is what it'd look like.
        setMockResponse(200, "{" +
                    "\"result\": [{" +
                        "\"category\": \"first\"," +
                        "\"plain_old_count\": 17," +
                        "\"style\": \"style1\"" +
                    "}, {" +
                        "\"category\": \"second\"," +
                        "\"plain_old_count\": 31," +
                        "\"style\": \"style2\"" +
                    "}]" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withGroupBy("category")
                .withGroupBy("style")
                .build();

        ObjectNode requestNode = getMultiAnalysisRequestNode(multiAnalysisParams);

        // Should have 'event_collection', 'analyses', 'timeframe' and 'group_by' top-level keys
        assertEquals(4, requestNode.size());

        // Validate the 'group_by' is there now with two parameters.
        // We always JSONify a collection for groupBy, so this should be an array.
        ArrayNode groupByNode = (ArrayNode)requestNode.get(KeenQueryConstants.GROUP_BY);
        assertEquals(2, groupByNode.size()); // Should have two 'group_by' parameters here

        List<String> groupByParams = new ArrayList<String>(2);
        Collections.addAll(groupByParams, "category", "style");

        for (JsonNode groupByParam : groupByNode) {
            assertTrue(groupByParams.remove(groupByParam.asText()));
        }

        assertTrue(groupByParams.isEmpty());
    }

    @Test
    public void testMultiAnalysis_Interval() throws Exception {
        // Response doesn't really matter here, but this is what it'd look like.
        setMockResponse(200, "{" +
                    "\"result\": [{" +
                        "\"timeframe\": {" +
                            "\"end\": \"2016-10-22T00:00:00.000Z\"," +
                            "\"start\": \"2016-10-21T00:00:00.000Z\"" +
                        "}," +
                        "\"value\": {" +
                            "\"plain_old_count\": 17" +
                        "}" +
                    "}, {" +
                        "\"timeframe\": {" +
                            "\"end\": \"2016-10-23T00:00:00.000Z\"," +
                            "\"start\": \"2016-10-22T00:00:00.000Z\"" +
                        "}," +
                        "\"value\": {" +
                            "\"plain_old_count\": 31" +
                        "}" +
                    "}]" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withInterval("daily")
                .build();

        ObjectNode requestNode = getMultiAnalysisRequestNode(multiAnalysisParams);

        // Should have 'event_collection', 'analyses', 'timeframe' and 'interval' top-level keys
        assertEquals(4, requestNode.size());

        // Make sure the 'count' sub-analysis is still there even though we specified 'group_by'...
        ObjectNode analysesNode = (ObjectNode)requestNode.get(KeenQueryConstants.ANALYSES);
        assertEquals(1, analysesNode.size());
        ObjectNode countNode = (ObjectNode)analysesNode.get("plain_old_count");
        assertEquals(1, countNode.size());
        assertNull("There should be no 'target_property' for 'count' analysis.",
                countNode.get(KeenQueryConstants.TARGET_PROPERTY));
        assertEquals(KeenQueryConstants.COUNT,
                countNode.get(KeenQueryConstants.ANALYSIS_TYPE).asText());

        // ...but also make sure the 'interval' is there now. It should just be a string.
        assertEquals("daily", requestNode.get(KeenQueryConstants.INTERVAL).asText());
    }

    @Test
    public void testMultiAnalysis_GroupAndInterval() throws Exception {
        // Response doesn't really matter here
        setMockResponse(200, "{\"result\": \"much stuff\"}");

        // This analysis has two sub-analyses, an interval, and two group by parameters.
        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withSubAnalysis(new SubAnalysis("total_cost", QueryType.SUM, "price"))
                .withInterval("daily")
                .withGroupBy("category")
                .withGroupBy("style")
                .build();

        ObjectNode requestNode = getMultiAnalysisRequestNode(multiAnalysisParams);

        // Should have 'event_collection', 'analyses', 'timeframe', 'interval' and 'group_by'
        // top-level keys.
        assertEquals(5, requestNode.size());

        // Make sure the 'sum' sub-analysis is still there even though we specified 'group_by' and
        // 'interval'.
        ObjectNode analysesNode = (ObjectNode)requestNode.get(KeenQueryConstants.ANALYSES);
        assertEquals(2, analysesNode.size());
        ObjectNode totalNode = (ObjectNode)analysesNode.get("total_cost");
        assertEquals(2, totalNode.size());
        assertEquals("price", totalNode.get(KeenQueryConstants.TARGET_PROPERTY).asText());
        assertEquals(KeenQueryConstants.SUM,
                totalNode.get(KeenQueryConstants.ANALYSIS_TYPE).asText());

        // ...but also make sure the 'interval' is there now. It should just be a string.
        assertEquals("daily", requestNode.get(KeenQueryConstants.INTERVAL).asText());

        // Validate the two 'group_by' parameters.
        ArrayNode groupByNode = (ArrayNode)requestNode.get(KeenQueryConstants.GROUP_BY);
        assertEquals(2, groupByNode.size()); // Should have two 'group_by' parameters here

        List<String> groupByParams = new ArrayList<String>(2);
        Collections.addAll(groupByParams, "category", "style");

        for (JsonNode groupByParam : groupByNode) {
            assertTrue(groupByParams.remove(groupByParam.asText()));
        }

        assertTrue(groupByParams.isEmpty());
    }

    @Test
    public void testMultiAnalysisResponse_Simple() throws Exception {
        setMockResponse(200, "{" +
                    "\"result\": {" +
                        "\"plain_old_count\": 24" +
                    "}" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .build();

        QueryResult result = queryClient.execute(multiAnalysisParams);

        assertTrue(result instanceof MultiAnalysisResult);
        MultiAnalysisResult multiAnalysisResult = (MultiAnalysisResult)result;
        assertEquals(1, multiAnalysisResult.getAllResults().size());
        assertEquals(24, multiAnalysisResult.getResultFor("plain_old_count").longValue());
    }

    @Test
    public void testMultiAnalysisResponse_GroupBy() throws Exception {
        final List<String> categories = Arrays.asList("first", "second");
        final String groupBy = "category";

        setMockResponse(200, "{" +
                    "\"result\": [{" +
                        "\"" + groupBy + "\": \"" + categories.get(0) + "\"," +
                        "\"plain_old_count\": 17" +
                    "}, {" +
                        "\"" + groupBy + "\": \"" + categories.get(1) + "\"," +
                        "\"plain_old_count\": 31" +
                    "}]" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withGroupBy("category")
                .build();

        QueryResult result = queryClient.execute(multiAnalysisParams);

        assertTrue(result.isGroupResult());

        for (Map.Entry<Group, QueryResult> groupResult : result.getGroupResults().entrySet()) {
            // Validate the GroupByResult
            Group group = groupResult.getKey();
            assertEquals(1, group.getProperties().size());
            assertTrue(group.getPropertyNames().contains(groupBy));
            assertTrue(categories.contains(group.getGroupValue(groupBy)));
            assertTrue(groupResult.getValue() instanceof MultiAnalysisResult);

            // Validate the actual MultiAnalysisResult
            MultiAnalysisResult multiAnalysisResult = (MultiAnalysisResult)groupResult.getValue();
            assertEquals(1, multiAnalysisResult.getAllResults().size());
            long resultForThisGroup =
                    multiAnalysisResult.getResultFor("plain_old_count").longValue();

            if (categories.get(0).equals(group.getGroupValue(groupBy))) {
                assertEquals(17, resultForThisGroup);
            } else {
                assertEquals(31, resultForThisGroup);
            }
        }
    }

    @Test
    public void testMultiAnalysisResponse_MultipleGroupBy() throws Exception {
        final List<String> categories = Arrays.asList("first", "second");
        final List<String> styles = Arrays.asList("style1", "style2", "style3");
        final String groupBy1 = "category";
        final String groupBy2 = "style";

        setMockResponse(200, "{" +
                    "\"result\": [{" +
                        "\"" + groupBy1 + "\": \"" + categories.get(0) + "\"," +
                        "\"plain_old_count\": 17," +
                        "\"" + groupBy2 + "\": \"" + styles.get(0) + "\"," +
                        "\"total_cost\": 143.45" +
                    "}, {" +
                        "\"" + groupBy1 + "\": \"" + categories.get(1) + "\"," +
                        "\"plain_old_count\": 31," +
                        "\"" + groupBy2 + "\": \"" + styles.get(1) + "\"," +
                        "\"total_cost\": 233.0 " +
                    "}]" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withSubAnalysis(new SubAnalysis("total_cost", QueryType.SUM, "price"))
                .withGroupBy("category")
                .withGroupBy("style")
                .build();

        QueryResult result = queryClient.execute(multiAnalysisParams);

        assertTrue(result.isGroupResult());
        assertEquals(2, result.getGroupResults().size()); // There are two groups.

        for (Map.Entry<Group, QueryResult> groupResult : result.getGroupResults().entrySet()) {
            // Validate the GroupByResult
            Group group = groupResult.getKey();
            assertEquals(2, group.getProperties().size());

            assertTrue(group.getPropertyNames().contains(groupBy1));
            assertTrue(categories.contains(group.getGroupValue(groupBy1)));
            assertTrue(group.getPropertyNames().contains(groupBy2));
            assertTrue(styles.contains(group.getGroupValue(groupBy2)));

            assertTrue(groupResult.getValue() instanceof MultiAnalysisResult);

            // Validate the actual MultiAnalysisResult
            MultiAnalysisResult multiAnalysisResult = (MultiAnalysisResult)groupResult.getValue();
            assertEquals(2, multiAnalysisResult.getAllResults().size());
            long countForThisGroup =
                    multiAnalysisResult.getResultFor("plain_old_count").longValue();

            double totalForThisGroup =
                    multiAnalysisResult.getResultFor("total_cost").doubleValue();

            if (categories.get(0).equals(group.getGroupValue(groupBy1))) {
                assertEquals(17, countForThisGroup);
                assertEquals(143.45, totalForThisGroup, 0.0);
            } else {
                assertEquals(31, countForThisGroup);
                assertEquals(233, totalForThisGroup, 0.0);
            }
        }
    }

    @Test
    public void testMultiAnalysisResponse_Interval() throws Exception {
        setMockResponse(200, "{" +
                    "\"result\": [{" +
                        "\"timeframe\": {" +
                            "\"end\": \"2016-10-22T00:00:00.000Z\"," +
                            "\"start\": \"2016-10-21T00:00:00.000Z\"" +
                        "}," +
                        "\"value\": {" +
                            "\"plain_old_count\": 17" +
                        "}" +
                    "}, {" +
                        "\"timeframe\": {" +
                            "\"end\": \"2016-10-23T00:00:00.000Z\"," +
                            "\"start\": \"2016-10-22T00:00:00.000Z\"" +
                        "}," +
                        "\"value\": {" +
                            "\"plain_old_count\": 31" +
                        "}" +
                    "}]" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withInterval("daily")
                .build();

        QueryResult result = queryClient.execute(multiAnalysisParams);

        assertTrue(result.isIntervalResult());
        assertEquals(2, result.getIntervalResults().size()); // There are two intervals.
        int intervalNum = 0;

        for (IntervalResultValue value : result.getIntervalResults()) {
            // Do some simple validation of the timeframe.
            AbsoluteTimeframe timeframe = value.getTimeframe();
            ZonedDateTime start = Instant.parse(timeframe.getStart()).atZone(ZoneId.of("UTC"));
            ZonedDateTime end = Instant.parse(timeframe.getEnd()).atZone(ZoneId.of("UTC"));
            assertEquals(Month.OCTOBER, start.getMonth());
            assertEquals(2016, end.getYear());
            assertTrue(start.isBefore(end));

            // Validate the QueryResult
            assertTrue(value.getResult() instanceof MultiAnalysisResult);
            MultiAnalysisResult multiAnalysisResult = (MultiAnalysisResult)value.getResult();

            assertEquals(1, multiAnalysisResult.getAllResults().size());
            long countForThisInterval =
                    multiAnalysisResult.getResultFor("plain_old_count").longValue();

            // Intervals, unlike groups and results for sub-analyses, actually come back in order,
            // or at least it seems like they do.
            if (0 == intervalNum) {
                assertEquals(17, countForThisInterval);
            } else if (1 == intervalNum) {
                assertEquals(31, countForThisInterval);
            } else {
                fail("More intervals than expected.");
            }

            intervalNum++;
        }
    }

    @Test
    public void testMultiAnalysisResponse_GroupAndInterval() throws Exception {
        final List<String> categories = Arrays.asList("first", "second");
        final List<String> styles = Arrays.asList("style1", "style2", "style3");
        final String groupBy1 = "category";
        final String groupBy2 = "style";

        setMockResponse(200, "{" +
                    "\"result\": [{" +
                        "\"timeframe\": {" +
                            "\"end\": \"2016-10-22T00:00:00.000Z\"," +
                            "\"start\": \"2016-10-21T00:00:00.000Z\"" +
                        "}," +
                        "\"value\": [{" +
                            "\"" + groupBy1 + "\": \"" + categories.get(0) + "\"," +
                            "\"plain_old_count\": 17," +
                            "\"" + groupBy2 + "\": \"" + styles.get(0) + "\"," +
                            "\"total_cost\": 143.45" +
                        "}, {" +
                            "\"" + groupBy1 + "\": \"" + categories.get(1) + "\"," +
                            "\"plain_old_count\": 31," +
                            "\"" + groupBy2 + "\": \"" + styles.get(1) + "\"," +
                            "\"total_cost\": 233.0" +
                        "}]" +
                    "}, {" + // End of first IntervalResult which holds two GroupByResults
                        "\"timeframe\": {" +
                            "\"end\": \"2016-10-23T00:00:00.000Z\"," +
                            "\"start\": \"2016-10-22T00:00:00.000Z\"" +
                        "}," +
                        "\"value\": [{" +
                            "\"" + groupBy1 + "\": \"" + categories.get(0) + "\"," +
                            "\"plain_old_count\": 18," +
                            "\"" + groupBy2 + "\": \"" + styles.get(0) + "\"," +
                            "\"total_cost\": 144.45" +
                        "}, {" +
                            "\"" + groupBy1 + "\": \"" + categories.get(1) + "\"," +
                            "\"plain_old_count\": 32," +
                            "\"" + groupBy2 + "\": \"" + styles.get(1) + "\"," +
                            "\"total_cost\": 234.0" +
                        "}]" +
                    "}]" +
                "}");

        // This analysis has two sub-analyses, an interval, and two group by parameters.
        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withSubAnalysis(new SubAnalysis("total_cost", QueryType.SUM, "price"))
                .withInterval("daily")
                .withGroupBy("category")
                .withGroupBy("style")
                .build();

        QueryResult result = queryClient.execute(multiAnalysisParams);

        assertTrue(result.isIntervalResult());
        assertEquals(2, result.getIntervalResults().size()); // Two intervals
        int intervalNum = 0;

        // A lot of this code is shared across the GroupBy, MultipleGroupBy, Interval and
        // GroupAndInterval tests, and could be combined into helpers, especially as we go to add
        // more tests.
        for (IntervalResultValue value : result.getIntervalResults()) {
            // Do some simple validation of the timeframe.
            AbsoluteTimeframe timeframe = value.getTimeframe();
            ZonedDateTime start = Instant.parse(timeframe.getStart()).atZone(ZoneId.of("UTC"));
            ZonedDateTime end = Instant.parse(timeframe.getEnd()).atZone(ZoneId.of("UTC"));
            assertEquals(Month.OCTOBER, start.getMonth());
            assertEquals(2016, end.getYear());
            assertTrue(start.isBefore(end));

            // Validate the nested GroupResults

            QueryResult groupResultInInterval = value.getResult();
            assertTrue(groupResultInInterval.isGroupResult());
            assertEquals(2, groupResultInInterval.getGroupResults().size()); // Two groups

            for (Map.Entry<Group, QueryResult> groupResult
                    : groupResultInInterval.getGroupResults().entrySet()) {
                // Validate each nested GroupByResult
                Group group = groupResult.getKey();
                assertEquals(2, group.getProperties().size());

                assertTrue(group.getPropertyNames().contains(groupBy1));
                assertTrue(categories.contains(group.getGroupValue(groupBy1)));
                assertTrue(group.getPropertyNames().contains(groupBy2));
                assertTrue(styles.contains(group.getGroupValue(groupBy2)));

                assertTrue(groupResult.getValue() instanceof MultiAnalysisResult);
                MultiAnalysisResult multiAnalysisResult =
                        (MultiAnalysisResult)groupResult.getValue();
                assertEquals(2, multiAnalysisResult.getAllResults().size());

                long countForThisGroup =
                        multiAnalysisResult.getResultFor("plain_old_count").longValue();

                double totalForThisGroup =
                        multiAnalysisResult.getResultFor("total_cost").doubleValue();

                if (categories.get(0).equals(group.getGroupValue(groupBy1))) {
                    assertEquals(17 + intervalNum, countForThisGroup);
                    assertEquals(143.45 + intervalNum, totalForThisGroup, 0.0);
                } else {
                    assertEquals(31 + intervalNum, countForThisGroup);
                    assertEquals(233 + intervalNum, totalForThisGroup, 0.0);
                }
            }

            intervalNum++;
        }
    }

    private static void validateMultiAnalysisRequiredFields(ObjectNode requestNode) {
        // Should have 'event_collection', 'analyses' and 'timeframe' top-level keys, at least.
        assertTrue("Missing required top-level fields.", 3 <= requestNode.size());
        assertEquals(TEST_EVENT_COLLECTION,
                     requestNode.get(KeenQueryConstants.EVENT_COLLECTION).asText());
        assertEquals("this_8_hours", requestNode.get(KeenQueryConstants.TIMEFRAME).asText());
    }

    private ObjectNode getMultiAnalysisRequestNode(MultiAnalysis multiAnalysisParams)
            throws Exception {
        ObjectNode requestNode = getRequestNode(multiAnalysisParams);

        validateMultiAnalysisRequiredFields(requestNode);

        return requestNode;
    }
}
