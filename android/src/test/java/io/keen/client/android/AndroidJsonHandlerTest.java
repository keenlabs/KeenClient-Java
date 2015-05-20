package io.keen.client.android;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.robolectric.RobolectricTestRunner;
import org.robolectric.annotation.Config;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the AndroidJsonHandler class.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.2
 */
@Config(emulateSdk = 18)
@RunWith(RobolectricTestRunner.class)
public class AndroidJsonHandlerTest {

    private AndroidJsonHandler handler;

    @Before
    public void createJsonHandler() {
        handler = new AndroidJsonHandler();
        handler.setWrapNestedMapsAndCollections(true);
    }

    @After
    public void cleanupJsonHandler() {
        handler = null;
    }

    @Test
    public void writeSimpleMap() throws Exception {
        // Build a simple map.
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("string_property", "value1");
        map.put("numeric_property", 10);
        map.put("boolean_property", true);

        // Serialize the map.
        String result = serialize(map);

        // Validate the result.
        assertTrue(result.contains("\"string_property\":\"value1\""));
        assertTrue(result.contains("\"numeric_property\":10"));
        assertTrue(result.contains("\"boolean_property\":true"));
    }

    @Test
    public void writeNestedMap() throws Exception {
        // Build a nested map.
        Map<String, Object> map = new HashMap<String, Object>();
        Map<String, Object> nested = new HashMap<String, Object>();
        nested.put("nested", "value");
        map.put("keen", nested);

        // Serialize the map.
        String result = serialize(map);

        // Validate the result.
        assertEquals("{\"keen\":{\"nested\":\"value\"}}", result);
    }

    @Test
    public void writeDeeplyNestedMap() throws Exception {
        // Build a deeply nested map.
        Map<String, Object> map = new HashMap<String, Object>();
        Map<String, Object> nested1 = new HashMap<String, Object>();
        Map<String, Object> nested2 = new HashMap<String, Object>();
        Map<String, Object> nested3 = new HashMap<String, Object>();
        Map<String, Object> nested4 = new HashMap<String, Object>();
        nested4.put("e", "value");
        nested3.put("d", nested4);
        nested2.put("c", nested3);
        nested1.put("b", nested2);
        map.put("a", nested1);

        // Serialize the map.
        String result = serialize(map);

        // Validate the result.
        assertEquals("{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":\"value\"}}}}}", result);
    }

    @Test
    public void writeMapWithCollection() throws Exception {
        // Build a map with a collection.
        Map<String, Object> map = new HashMap<String, Object>();
        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        list.add("c");
        map.put("list", list);

        // Serialize the map.
        String result = serialize(map);

        // Validate the result.
        assertEquals("{\"list\":[\"a\",\"b\",\"c\"]}", result);
    }

    @Test
    public void writeEvillyNestedMapsAndCollections() throws Exception {
        // Build an evil mix of nested maps and collections.
        Map<String, Object> map1 = new HashMap<String, Object>();
        map1.put("a", "1");
        map1.put("b", "2");
        List<Object> list1 = new ArrayList<Object>();
        list1.add("c");
        Map<String, Object> map2 = new HashMap<String, Object>();
        map2.put("d", "4");
        map2.put("e", "5");
        List<Object> list2 = new ArrayList<Object>();
        list2.add("f");
        Map<String, Object> map3 = new HashMap<String, Object>();
        map3.put("g", "7");
        List<Object> list3 = new ArrayList<Object>();
        list3.add("h");
        list3.add("i");
        map3.put("j", list3);
        map3.put("k", "11");
        list2.add(map3);
        list2.add("l");
        map2.put("m", list2);
        map2.put("n", "14");
        list1.add(map2);
        map1.put("o", list1);

        // Serialize the map.
        String result = serialize(map1);

        // Validate the result.
        assertThat(result, containsString("\"j\":[\"h\",\"i\"]"));
    }

    private String serialize(Map<String, Object> map) throws Exception {
        // Build a string writer to receive the serialized output.
        StringWriter writer = new StringWriter();

        // Serialize the map using an AndroidJsonHandler, and extract the resulting string.
        handler.writeJson(writer, map);
        String result = writer.toString();

        // Close the writer and return the string.
        writer.close();
        return result;
    }

}
