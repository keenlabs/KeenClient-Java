package io.keen.client.java;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.keen.client.java.exceptions.KeenException;
import io.keen.client.java.exceptions.NoWriteKeyException;
import io.keen.client.java.exceptions.ServerException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * KeenClientTest
 *
 * @author dkador
 * @since 1.0.0
 */
public class KeenClientTest {

    private static class TestKeenClient extends KeenClient {
        @Override
        public KeenJsonHandler getJsonHandler() {
            return jsonHandler;
        }

        @Override
        public KeenEventStore getEventStore() {
            return eventStore;
        }

        @Override
        public Executor getPublishExecutor() {
            return publishExecutor;
        }

        private final KeenJsonHandler jsonHandler;
        private final KeenEventStore eventStore;
        private final Executor publishExecutor;

        TestKeenClient() {
            jsonHandler = new KeenJsonHandler() {
                @Override
                public Map<String, Object> readJson(Reader reader) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void writeJson(Writer writer, Map<String, ?> value) throws IOException {
                    throw new UnsupportedOperationException();
                }
            };
            eventStore = new RamEventStore();
            publishExecutor = Executors.newSingleThreadExecutor();
        }
    }

    private static final KeenProject TEST_PROJECT = new KeenProject("508339b0897a2c4282000000",
            "80ce00d60d6443118017340c42d1cfaf", "80ce00d60d6443118017340c42d1cfaf");

    @BeforeClass
    public static void classSetUp() {
        KeenLogging.enableLogging();
        KeenClient client = new TestKeenClient();
        client.setDebugMode(true);
        KeenClient.initialize(client);
    }

    @Before
    public void setup() {
        KeenClient.client().setDefaultProject(TEST_PROJECT);
    }

    @Test
    public void testInvalidEventCollection() throws KeenException {
        runValidateAndBuildEventTest(TestUtils.getSimpleEvent(), "$asd", "collection can't start with $",
                                     "An event collection name cannot start with the dollar sign ($) character.");

        String tooLong = TestUtils.getString(257);
        runValidateAndBuildEventTest(TestUtils.getSimpleEvent(), tooLong, "collection can't be longer than 256 chars",
                                     "An event collection name cannot be longer than 256 characters.");
    }

    // TODO: Test for self-referential event, event with list.
    @Test
    public void nullEvent() throws Exception {
        runValidateAndBuildEventTest(null, "foo", "null event",
                                     "You must specify a non-null, non-empty event.");
    }

    @Test
    public void emptyEvent() throws Exception {
        runValidateAndBuildEventTest(new HashMap<String, Object>(), "foo", "empty event",
                                     "You must specify a non-null, non-empty event.");
    }

    @Test
    public void eventWithKeenRootProperty() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("keen", "reserved");
        runValidateAndBuildEventTest(event, "foo", "keen reserved",
                "An event cannot contain a root-level property named 'keen'.");
    }

    @Test
    public void eventWithDotInPropertyName() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("ab.cd", "whatever");
        runValidateAndBuildEventTest(event, "foo", ". in property name",
                                     "An event cannot contain a property with the period (.) character in it.");
    }

    @Test
    public void eventWithInitialDollarPropertyName() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("$a", "whatever");
        runValidateAndBuildEventTest(event, "foo", "$ at start of property name",
                                     "An event cannot contain a property that starts with the dollar sign ($) " +
                                             "character in it.");
    }

    @Test
    public void eventWithTooLongPropertyName() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        String tooLong = TestUtils.getString(257);
        event.put(tooLong, "whatever");
        runValidateAndBuildEventTest(event, "foo", "too long property name",
                                     "An event cannot contain a property name longer than 256 characters.");
    }

    @Test
    public void eventWithTooLongStringValue() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        String tooLong = TestUtils.getString(10000);
        event.put("long", tooLong);
        runValidateAndBuildEventTest(event, "foo", "too long property value",
                "An event cannot contain a string property value longer than 10,000 characters.");
    }

    @Test
    public void eventWithSelfReferencingProperty() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("recursion", event);
        runValidateAndBuildEventTest(event, "foo", "self referencing",
                "An event's depth (i.e. layers of nesting) cannot exceed " + KeenConstants.MAX_EVENT_DEPTH);
    }

    @Test
    public void eventWithInvalidPropertyInList() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        List<String> invalidList = new ArrayList<String>();
        String tooLong = TestUtils.getString(10000);
        invalidList.add(tooLong);
        event.put("invalid_list", invalidList);
        runValidateAndBuildEventTest(event, "foo", "invalid value in list",
                "An event cannot contain a string property value longer than 10,000 characters.");
    }

    @Test
    public void validEvent() throws Exception {
        KeenClient client = KeenClient.client();
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("valid key", "valid value");
        Map<String, Object> builtEvent = client.validateAndBuildEvent(client.getDefaultProject(), "foo", event, null);
        assertNotNull(builtEvent);
        assertEquals("valid value", builtEvent.get("valid key"));
        // also make sure the event has been timestamped
        @SuppressWarnings("unchecked")
        Map<String, Object> keenNamespace = (Map<String, Object>) builtEvent.get("keen");
        assertNotNull(keenNamespace);
        assertNotNull(keenNamespace.get("timestamp"));
    }

    @Test
    public void validEventWithTimestamp() throws Exception {
        KeenClient client = KeenClient.client();
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("valid key", "valid value");
        Calendar now = Calendar.getInstance();
        event.put("datetime", now);
        client.validateAndBuildEvent(client.getDefaultProject(), "foo", event, null);
    }

    @Test
    public void validEventWithNestedKeenProperty() throws Exception {
        KeenClient client = KeenClient.client();
        Map<String, Object> event = TestUtils.getSimpleEvent();
        Map<String, Object> nested = new HashMap<String, Object>();
        nested.put("keen", "value");
        event.put("nested", nested);
        client.validateAndBuildEvent(client.getDefaultProject(), "foo", event, null);
    }

    @Test
    public void testAddEventNoWriteKey() throws KeenException, IOException {
        // TODO: Don't special-case using debug mode here.
        KeenClient client = KeenClient.client();
        client.setDefaultProject(new KeenProject("508339b0897a2c4282000000", null, null));
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("test key", "test value");
        try {
            client.addEvent("foo", event);
            fail("add event without write key should fail");
        } catch (NoWriteKeyException e) {
            assertEquals("You can't send events to Keen IO if you haven't set a write key.",
                    e.getLocalizedMessage());
        }
    }

    @Test
    public void testResponseProcessing() throws Exception {
        runResponseProcessingTest("blah", null, 200, null);
        runResponseProcessingTest("blah", null, 201, null);
        runResponseProcessingTest(null, "blah", 400, "blah");
        runResponseProcessingTest(null, "blah", 500, "blah");
    }

    private void runResponseProcessingTest(String response, String error, int statusCode,
                                           String expectedError) throws Exception {
        InputStream responseStream = null;
        if (response != null) {
            responseStream = new ByteArrayInputStream(response.getBytes("UTF-8"));
        }

        InputStream errorStream = null;
        if (error != null) {
            errorStream = new ByteArrayInputStream(error.getBytes("UTF-8"));
        }

        try {
            KeenClient.client().processConnectionResponse(statusCode, responseStream, errorStream);
            if (expectedError != null) {
                fail("Expected error '" + expectedError + "' but succeeded");
            }
        } catch (Exception e) {
            if (expectedError == null) {
                fail("Expected success but got error '" + e.getMessage() + "' of type '" +
                    e.getClass() + "'");
            } else {
                assertTrue(e instanceof ServerException);
                assertEquals(expectedError, e.getMessage());
            }
        }
    }

    @Test
    public void testGlobalPropertiesMap() throws Exception {
        // a null map should be okay
        runGlobalPropertiesMapTest(null, 1);

        // an empty map should be okay
        runGlobalPropertiesMapTest(new HashMap<String, Object>(), 1);

        // a map w/ non-conflicting property names should be okay
        Map<String, Object> globals = new HashMap<String, Object>();
        globals.put("default name", "default value");
        Map<String, Object> builtEvent = runGlobalPropertiesMapTest(globals, 2);
        assertEquals("default value", builtEvent.get("default name"));

        // a map that returns a conflicting property name should not overwrite the property on the event
        globals = new HashMap<String, Object>();
        globals.put("a", "c");
        builtEvent = runGlobalPropertiesMapTest(globals, 1);
        assertEquals("b", builtEvent.get("a"));
    }

    private Map<String, Object> runGlobalPropertiesMapTest(Map<String, Object> globalProperties,
                                                           int expectedNumProperties) throws Exception {
        KeenClient client = KeenClient.client();
        client.setGlobalProperties(globalProperties);
        Map<String, Object> event = TestUtils.getSimpleEvent();
        String eventCollection = String.format("foo%d", Calendar.getInstance().getTimeInMillis());
        Map<String, Object> builtEvent = client.validateAndBuildEvent(client.getDefaultProject(), eventCollection, event, null);
        assertEquals(expectedNumProperties + 1, builtEvent.size());
        return builtEvent;
    }

    @Test
    public void testGlobalPropertiesEvaluator() throws Exception {
        // a null evaluator should be okay
        runGlobalPropertiesEvaluatorTest(null, 1);

        // an evaluator that returns an empty map should be okay
        GlobalPropertiesEvaluator evaluator = new GlobalPropertiesEvaluator() {
            @Override
            public Map<String, Object> getGlobalProperties(String eventCollection) {
                return new HashMap<String, Object>();
            }
        };
        runGlobalPropertiesEvaluatorTest(evaluator, 1);

        // an evaluator that returns a map w/ non-conflicting property names should be okay
        evaluator = new GlobalPropertiesEvaluator() {
            @Override
            public Map<String, Object> getGlobalProperties(String eventCollection) {
                Map<String, Object> globals = new HashMap<String, Object>();
                globals.put("default name", "default value");
                return globals;
            }
        };
        Map<String, Object> builtEvent = runGlobalPropertiesEvaluatorTest(evaluator, 2);
        assertEquals("default value", builtEvent.get("default name"));

        // an evaluator that returns a map w/ conflicting property name should not overwrite the property on the event
        evaluator = new GlobalPropertiesEvaluator() {
            @Override
            public Map<String, Object> getGlobalProperties(String eventCollection) {
                Map<String, Object> globals = new HashMap<String, Object>();
                globals.put("a", "c");
                return globals;
            }
        };
        builtEvent = runGlobalPropertiesEvaluatorTest(evaluator, 1);
        assertEquals("b", builtEvent.get("a"));
    }

    private Map<String, Object> runGlobalPropertiesEvaluatorTest(GlobalPropertiesEvaluator evaluator,
                                                                 int expectedNumProperties) throws Exception {
        KeenClient client = KeenClient.client();
        client.setGlobalPropertiesEvaluator(evaluator);
        Map<String, Object> event = TestUtils.getSimpleEvent();
        String eventCollection = String.format("foo%d", Calendar.getInstance().getTimeInMillis());
        Map<String, Object> builtEvent = client.validateAndBuildEvent(client.getDefaultProject(), eventCollection, event, null);
        assertEquals(expectedNumProperties + 1, builtEvent.size());
        return builtEvent;
    }

    @Test
    public void testGlobalPropertiesTogether() throws Exception {
        KeenClient client = KeenClient.client();

        // properties from the evaluator should take precedence over properties from the map
        // but properties from the event itself should take precedence over all
        Map<String, Object> globalProperties = new HashMap<String, Object>();
        globalProperties.put("default property", 5);
        globalProperties.put("foo", "some new value");

        GlobalPropertiesEvaluator evaluator = new GlobalPropertiesEvaluator() {
            @Override
            public Map<String, Object> getGlobalProperties(String eventCollection) {
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("default property", 6);
                map.put("foo", "some other value");
                return map;
            }
        };

        client.setGlobalProperties(globalProperties);
        client.setGlobalPropertiesEvaluator(evaluator);

        Map<String, Object> event = new HashMap<String, Object>();
        event.put("foo", "bar");
        Map<String, Object> builtEvent = client.validateAndBuildEvent(client.getDefaultProject(), "apples", event, null);

        assertEquals("bar", builtEvent.get("foo"));
        assertEquals(6, builtEvent.get("default property"));
        assertEquals(3, builtEvent.size());
    }

    private void runValidateAndBuildEventTest(Map<String, Object> event, String eventCollection, String msg,
                                              String expectedMessage) {
        KeenClient client = KeenClient.client();
        try {
            client.validateAndBuildEvent(client.getDefaultProject(), eventCollection, event, null);
            fail(msg);
        } catch (KeenException e) {
            assertEquals(expectedMessage, e.getLocalizedMessage());
        }
    }

}
