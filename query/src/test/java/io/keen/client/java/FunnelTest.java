package io.keen.client.java;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import io.keen.client.java.exceptions.KeenQueryClientException;
import io.keen.client.java.http.Request;
import io.keen.client.java.result.FunnelResult;
import io.keen.client.java.result.ListResult;
import io.keen.client.java.result.QueryResult;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;

/**
 * Test the Funnel query functionality.
 *
 * @author baumatron, masojus
 */
public class FunnelTest extends KeenQueryTestBase {

    private FilterOperator stringToFilterOperator(String operator) {
        return FilterOperator.fromString(operator);
    }

    private List<FunnelStep> buildFunnelStepsFromRequestJson(JsonNode requestJson) {

        JsonNode stepsJson = requestJson.findValue("steps");

        // Construct a list of funnel steps based on provided data
        List<FunnelStep> funnelSteps = new ArrayList<FunnelStep>();

        for (JsonNode stepJson : stepsJson) {
            Timeframe timeframe = null;
            List<Filter> filters = null;
            Boolean inverted = null;
            Boolean optional = null;
            Boolean withActors = null;

            if (stepJson.has(KeenQueryConstants.TIMEFRAME) &&
                    stepJson.has(KeenQueryConstants.TIMEZONE)) {
                timeframe = new RelativeTimeframe(
                        stepJson.get(KeenQueryConstants.TIMEFRAME).asText(),
                        stepJson.get(KeenQueryConstants.TIMEZONE).asText());
            } else if(stepJson.has(KeenQueryConstants.TIMEFRAME)) {
                JsonNode timeframeJson = stepJson.get(KeenQueryConstants.TIMEFRAME);
                if (!timeframeJson.isObject()) {
                    timeframe = new RelativeTimeframe(timeframeJson.asText());
                } else {
                    throw new IllegalStateException(
                            "Building absolute timeframes isn't supported by this method.");
                }
            }

            if (stepJson.has(KeenQueryConstants.FILTERS)) {
                JsonNode filterListJson = stepJson.get(KeenQueryConstants.FILTERS);

                for (JsonNode filterJson : filterListJson) {
                    if (null == filters) {
                        filters = new LinkedList<Filter>();
                    }
                    filters.add(
                            new Filter(
                                    filterJson.get(KeenQueryConstants.PROPERTY_NAME).asText(),
                                    stringToFilterOperator(filterJson.get(KeenQueryConstants.OPERATOR).asText()),
                                    filterJson.get(KeenQueryConstants.PROPERTY_VALUE).asText()
                            )
                    );
                }
            }

            if (stepJson.has(KeenQueryConstants.INVERTED)) {
                inverted = stepJson.get(KeenQueryConstants.INVERTED).asBoolean();
            }

            if (stepJson.has(KeenQueryConstants.OPTIONAL)) {
                optional = stepJson.get(KeenQueryConstants.OPTIONAL).asBoolean();
            }

            if (stepJson.has(KeenQueryConstants.WITH_ACTORS)) {
                withActors = stepJson.get(KeenQueryConstants.WITH_ACTORS).asBoolean();
            }

            FunnelStep step = new FunnelStep(
                    stepJson.get(KeenQueryConstants.EVENT_COLLECTION).asText(),
                    stepJson.get(KeenQueryConstants.ACTOR_PROPERTY).asText(),
                    timeframe,
                    filters,
                    inverted,
                    optional,
                    withActors
            );

            funnelSteps.add(step);
        }

        return funnelSteps;
    }

    @Test
    public void testFunnelWithOnlyRequiredParameters() throws Exception {

        JsonNode expectedRequest = OBJECT_MAPPER.readTree(
                "{\"steps\":[{\"timeframe\":\"this_7_days\",\"actor_property\":\"visitor.guid\",\"event_collection\":\"signed up\"},"
                        + "{\"timeframe\":\"this_7_days\",\"actor_property\":\"user.guid\",\"event_collection\":\"completed profile\"},"
                        + "{\"timeframe\":\"this_7_days\",\"timezone\":\"UTC\",\"actor_property\":\"user.guid\",\"event_collection\":\"referred user\"}]}"
        );

        // Build the mock response based on provided data
        String mockResponse =
                "{\"result\": [3,1,0],\"steps\":"
                        + OBJECT_MAPPER.writeValueAsString(expectedRequest.findValue("steps"))
                        + "}";
        setMockResponse(200, mockResponse);

        // Construct a list of funnel steps based on provided data
        List<FunnelStep> funnelSteps = buildFunnelStepsFromRequestJson(expectedRequest);

        Funnel funnel = new Funnel.Builder()
                .withStep(funnelSteps.get(0))
                .withStep(funnelSteps.get(1))
                .withStep(funnelSteps.get(2))
                .build();

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        QueryResult result = queryClient.execute(funnel);
        assertTrue(result instanceof FunnelResult);
        FunnelResult funnelResult = (FunnelResult)result;

        // Capture the request for validation
        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestBody = outputStream.toString(ENCODING);

        // Validate request
        JsonNode requestNode = OBJECT_MAPPER.readTree(requestBody);
        assertTrue(expectedRequest.equals(requestNode));

        // Validate result
        ListResult funnelValues = funnelResult.getFunnelResult();
        List<QueryResult> funnelResultData = funnelValues.getListResults();
        assertTrue("Unexpected result value.", 3 == funnelResultData.get(0).longValue());
        assertTrue("Unexpected result value.", 1 == funnelResultData.get(1).longValue());
        assertTrue("Unexpected result value.", 0 == funnelResultData.get(2).longValue());
        assertTrue("Should not have actors result.", null == funnelResult.getActorsResult());
    }

    @Test
    public void testFunnelBuilderNonFluent() throws Exception {

        JsonNode expectedRequest = OBJECT_MAPPER.readTree(
                "{\"steps\":[{\"timeframe\":\"this_7_days\",\"actor_property\":\"visitor.guid\",\"event_collection\":\"signed up\"},"
                        + "{\"timeframe\":\"this_7_days\",\"actor_property\":\"user.guid\",\"event_collection\":\"completed profile\"},"
                        + "{\"timeframe\":\"this_7_days\",\"timezone\":\"UTC\",\"actor_property\":\"user.guid\",\"event_collection\":\"referred user\"}]}"
        );

        // Build the mock response based on provided data
        String mockResponse =
                "{\"result\": [3,1,0],\"steps\":"
                        + OBJECT_MAPPER.writeValueAsString(expectedRequest.findValue("steps"))
                        + "}";
        setMockResponse(200, mockResponse);

        // Construct a list of funnel steps based on provided data
        List<FunnelStep> funnelSteps = buildFunnelStepsFromRequestJson(expectedRequest);

        Funnel.Builder builder = new Funnel.Builder();
        builder.setSteps(funnelSteps);
        Funnel funnel = builder.build();

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        QueryResult result = queryClient.execute(funnel);
        assertTrue(result instanceof FunnelResult);
        FunnelResult funnelResult = (FunnelResult)result;

        // Capture the request for validation
        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestBody = outputStream.toString(ENCODING);

        // Validate request
        JsonNode requestNode = OBJECT_MAPPER.readTree(requestBody);
        assertTrue(expectedRequest.equals(requestNode));

        // Validate result
        ListResult funnelValues = funnelResult.getFunnelResult();
        List<QueryResult> funnelResultData = funnelValues.getListResults();
        assertTrue("Unexpected result value.", 3 == funnelResultData.get(0).longValue());
        assertTrue("Unexpected result value.", 1 == funnelResultData.get(1).longValue());
        assertTrue("Unexpected result value.", 0 == funnelResultData.get(2).longValue());
        assertTrue("Should not have actors result.", null == funnelResult.getActorsResult());
    }

    @Test
    public void testFunnelWithOnlyRootTimeframe() throws Exception {

        String rootTimeframeString = "this_7_days";

        JsonNode expectedRequest = OBJECT_MAPPER.readTree(
                "{\"timeframe\":\"" + rootTimeframeString + "\",\"steps\":["
                        + "{\"actor_property\":\"visitor.guid\",\"event_collection\":\"signed up\"},"
                        + "{\"actor_property\":\"user.guid\",\"event_collection\":\"completed profile\"},"
                        + "{\"actor_property\":\"user.guid\",\"event_collection\":\"referred user\"}]}"
        );

        // Build the mock response based on provided data
        String mockResponse =
                "{\"result\": [3,1,0],\"timeframe\":\"" + rootTimeframeString + "\",\"steps\":"
                        + OBJECT_MAPPER.writeValueAsString(expectedRequest.findValue("steps"))
                        + "}";
        setMockResponse(200, mockResponse);

        // Construct a list of funnel steps based on provided data
        List<FunnelStep> funnelSteps = buildFunnelStepsFromRequestJson(expectedRequest);

        Funnel funnel = new Funnel.Builder()
                .withTimeframe(new RelativeTimeframe(rootTimeframeString))
                .withStep(funnelSteps.get(0))
                .withStep(funnelSteps.get(1))
                .withStep(funnelSteps.get(2))
                .build();

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        QueryResult result = queryClient.execute(funnel);
        assertTrue(result instanceof FunnelResult);
        FunnelResult funnelResult = (FunnelResult)result;

        // Capture the request for validation
        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestBody = outputStream.toString(ENCODING);

        // Validate request
        JsonNode requestNode = OBJECT_MAPPER.readTree(requestBody);
        assertTrue(expectedRequest.equals(requestNode));

        // Validate result
        ListResult funnelValues = funnelResult.getFunnelResult();
        List<QueryResult> funnelResultData = funnelValues.getListResults();
        assertTrue("Unexpected result value.", 3 == funnelResultData.get(0).longValue());
        assertTrue("Unexpected result value.", 1 == funnelResultData.get(1).longValue());
        assertTrue("Unexpected result value.", 0 == funnelResultData.get(2).longValue());
        assertTrue("Should not have actors result.", null == funnelResult.getActorsResult());
    }

    @Test
    public void testFunnelBuilderEnsureWithStepsThrowsWithWrongUsage() throws Exception {

        String rootTimeframeString = "this_7_days";

        JsonNode expectedRequest = OBJECT_MAPPER.readTree(
                "{\"timeframe\":\"" + rootTimeframeString + "\",\"steps\":["
                        + "{\"actor_property\":\"visitor.guid\",\"event_collection\":\"signed up\"},"
                        + "{\"actor_property\":\"user.guid\",\"event_collection\":\"completed profile\"},"
                        + "{\"actor_property\":\"user.guid\",\"event_collection\":\"referred user\"}]}"
        );

        // Construct a list of funnel steps based on provided data
        List<FunnelStep> funnelSteps = buildFunnelStepsFromRequestJson(expectedRequest);

        boolean threwCorrectExceptionType = false;
        try {
            new Funnel.Builder()
                    .withStep(funnelSteps.get(0))
                    .withSteps(funnelSteps)
                    .build();
            fail("Expected KeenQueryClientException with incorrect usage of withStep" +
                         "combined with withSteps.");
        } catch (KeenQueryClientException keenException) {
            threwCorrectExceptionType = true;
        } catch (Exception e) {
            threwCorrectExceptionType = false;
        }
        assertTrue(threwCorrectExceptionType);
    }

    @Test
    public void testFunnelWithSpecialParameters() throws Exception {

        List<String> actorValues = new ArrayList<String>();
        actorValues.add("f9332409s0");
        actorValues.add("b7732409s0");
        actorValues.add("k22315b211");

        JsonNode expectedRequest = OBJECT_MAPPER.readTree(
                "{\"steps\":["
                        + "{\"timeframe\":\"this_7_days\",\"actor_property\":\"visitor.guid\",\"event_collection\":\"signed up\",\"with_actors\":true},"
                        + "{\"timeframe\":\"this_7_days\",\"actor_property\":\"user.guid\",\"event_collection\":\"completed profile\",\"inverted\":true},"
                        + "{\"timeframe\":\"this_7_days\",\"timezone\":\"UTC\",\"optional\":true,\"actor_property\":\"user.guid\",\"event_collection\":\"referred user\"}]}"
        );

        // Build the mock response based on provided data
        String mockResponse =
                "{\"result\": [3,2,1],"
                        + "\"actors\": [" + OBJECT_MAPPER.writeValueAsString(actorValues) + ", null, null],"
                        + "\"steps\":" + OBJECT_MAPPER.writeValueAsString(expectedRequest.findValue("steps"))
                        + "}";
        setMockResponse(200, mockResponse);

        // Construct a list of funnel steps based on provided data
        List<FunnelStep> funnelSteps = buildFunnelStepsFromRequestJson(expectedRequest);

        Funnel funnel = new Funnel.Builder()
                .withStep(funnelSteps.get(0))
                .withStep(funnelSteps.get(1))
                .withStep(funnelSteps.get(2))
                .build();

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        QueryResult result = queryClient.execute(funnel);
        assertTrue(result instanceof FunnelResult);
        FunnelResult funnelResult = (FunnelResult)result;

        // Capture the request for validation
        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestBody = outputStream.toString(ENCODING);

        // Validate request
        JsonNode requestNode = OBJECT_MAPPER.readTree(requestBody);
        assertTrue(expectedRequest.equals(requestNode));

        // Validate result
        ListResult funnelValues = funnelResult.getFunnelResult();
        List<QueryResult> funnelResultData = funnelValues.getListResults();
        assertTrue("Unexpected result value.", 3 == funnelResultData.get(0).longValue());
        assertTrue("Unexpected result value.", 2 == funnelResultData.get(1).longValue());
        assertTrue("Unexpected result value.", 1 == funnelResultData.get(2).longValue());
        ListResult actorResult = funnelResult.getActorsResult();
        List<QueryResult> actorResultList = actorResult.getListResults();
        List<QueryResult> firstStepActorList = actorResultList.get(0).getListResults();
        assertTrue("Unexpected actor value.", 0 == firstStepActorList.get(0).stringValue().compareTo(actorValues.get(0)));
        assertTrue("Unexpected actor value.", 0 == firstStepActorList.get(1).stringValue().compareTo(actorValues.get(1)));
        assertTrue("Unexpected actor value.", 0 == firstStepActorList.get(2).stringValue().compareTo(actorValues.get(2)));
        assertTrue("Unexpected actor result.", null == actorResultList.get(1));
        assertTrue("Unexpected actor result.", null == actorResultList.get(2));
    }

    @Test
    public void testFunnelWithFilters() throws Exception {

        JsonNode expectedRequest = OBJECT_MAPPER.readTree(
                "{\"steps\":[{\"timeframe\":\"this_7_days\",\"actor_property\":\"visitor.guid\","
                        + "\"filters\":[{\"property_value\":\"some_value\",\"operator\":\"eq\","
                        + "\"property_name\":\"some_name\"}],\"event_collection\":\"signed up\"},"
                        + "{\"timeframe\":\"this_7_days\",\"actor_property\":\"user.guid\","
                        + "\"event_collection\":\"completed profile\"},{\"timeframe\":\"this_7_days\","
                        + "\"timezone\":\"UTC\",\"actor_property\":\"user.guid\",\"event_collection\":"
                        + "\"referred user\"}]}"
        );

        // Build the mock response based on provided data
        String mockResponse =
                "{\"result\": [3,1,0],\"steps\":"
                        + OBJECT_MAPPER.writeValueAsString(expectedRequest.findValue("steps"))
                        + "}";
        setMockResponse(200, mockResponse);

        // Construct a list of funnel steps based on provided data
        List<FunnelStep> funnelSteps = buildFunnelStepsFromRequestJson(expectedRequest);

        Funnel funnel = new Funnel.Builder()
                .withStep(funnelSteps.get(0))
                .withStep(funnelSteps.get(1))
                .withStep(funnelSteps.get(2))
                .build();

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        QueryResult result = queryClient.execute(funnel);
        assertTrue(result instanceof FunnelResult);
        FunnelResult funnelResult = (FunnelResult)result;

        // Capture the request for validation
        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestBody = outputStream.toString(ENCODING);

        // Validate request
        JsonNode requestNode = OBJECT_MAPPER.readTree(requestBody);
        assertTrue(expectedRequest.equals(requestNode));

        // Validate result
        ListResult funnelValues = funnelResult.getFunnelResult();
        List<QueryResult> funnelResultData = funnelValues.getListResults();
        assertTrue("Unexpected result value.", 3 == funnelResultData.get(0).longValue());
        assertTrue("Unexpected result value.", 1 == funnelResultData.get(1).longValue());
        assertTrue("Unexpected result value.", 0 == funnelResultData.get(2).longValue());
        assertTrue("Should not have actors result.", null == funnelResult.getActorsResult());
    }

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testFunnelWithInvalidTimeframeConfiguration() throws Exception {
        exception.expect(IllegalArgumentException.class);
        new Funnel.Builder()
                .withStep(new FunnelStep("signed up", "visitor.guid"))
                .withStep(new FunnelStep("completed profile", "user.guid"))
                .withStep(new FunnelStep("referred user", "user.guid"))
                .build();
    }

    @Test
    public void testFunnelWithInvalidInvertedSpecialParameter() throws Exception {
        exception.expect(IllegalArgumentException.class);
        new Funnel.Builder()
                .withStep(new FunnelStep("signed up", "visitor.guid", new RelativeTimeframe("this_7_days"), null, true, null, null))
                .withStep(new FunnelStep("completed profile", "user.guid", new RelativeTimeframe("this_7_days")))
                .withStep(new FunnelStep("referred user", "user.guid", new RelativeTimeframe("this_7_days", "UTC")))
                .build();
    }

    @Test
    public void testFunnelWithInvalidOptionalSpecialParameter() throws Exception {
        exception.expect(IllegalArgumentException.class);
        new Funnel.Builder()
                .withStep(new FunnelStep("signed up", "visitor.guid", new RelativeTimeframe("this_7_days"), null, null, true, null))
                .withStep(new FunnelStep("completed profile", "user.guid", new RelativeTimeframe("this_7_days")))
                .withStep(new FunnelStep("referred user", "user.guid", new RelativeTimeframe("this_7_days", "UTC")))
                .build();
    }
}
