package io.keen.client.android;


import org.hamcrest.Matchers;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Tests the AndroidJsonHandler class.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.2
 */
public class AndroidJsonHandlerTest {

    private AndroidJsonHandler handler;

    @Mock
    private AndroidJsonHandler.JsonObjectManager mockJsonObjectManager;

    @Captor
    private ArgumentCaptor<Map<String, ?>> mapArgumentCaptor;

    @Captor
    private ArgumentCaptor<Collection<?>> collectionArgumentCaptor;

    @Before
    public void createJsonHandler() {
        MockitoAnnotations.initMocks(this);
        handler = new AndroidJsonHandler();
        handler.setWrapNestedMapsAndCollections(true);
        handler.setJsonObjectManager(mockJsonObjectManager);
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
        serialize(map);

        // Verify the calls.
        verify(mockJsonObjectManager).newObject(mapArgumentCaptor.capture());
        Map<String, ?> mapArgument = mapArgumentCaptor.getValue();
        assertThat(mapArgument.size(), equalTo(3));
        assertThat((String) mapArgument.get("string_property"), equalTo("value1"));
        assertThat((Integer) mapArgument.get("numeric_property"), equalTo(10));
        assertThat((Boolean) mapArgument.get("boolean_property"), equalTo(true));
        verify(mockJsonObjectManager).stringify(any(JSONObject.class));
        verifyNoMoreInteractions(mockJsonObjectManager);
    }

    @Test
    public void writeNestedMap() throws Exception {
        // Build a nested map.
        Map<String, Object> map = new HashMap<String, Object>();
        Map<String, Object> nested = new HashMap<String, Object>();
        nested.put("nested", "value");
        map.put("keen", nested);

        // Serialize the map.
        serialize(map);

        // Verify the calls.
        verify(mockJsonObjectManager, times(2)).newObject(mapArgumentCaptor.capture());
        List<Map<String, ?>> mapArguments = mapArgumentCaptor.getAllValues();
        assertThat(mapArguments.size(), equalTo(2));
        assertThat(mapArguments.get(0).size(), equalTo(1));
        assertThat((String) mapArguments.get(0).get("nested"), equalTo("value"));
        assertThat(mapArguments.get(1).size(), equalTo(1));
        assertThat(mapArguments.get(1).containsKey("keen"), equalTo(true));
        verify(mockJsonObjectManager).stringify(any(JSONObject.class));
        verifyNoMoreInteractions(mockJsonObjectManager);
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
        serialize(map);

        // Verify the calls.
        verify(mockJsonObjectManager, times(5)).newObject(mapArgumentCaptor.capture());
        List<Map<String, ?>> mapArguments = mapArgumentCaptor.getAllValues();
        assertThat(mapArguments.size(), equalTo(5));
        assertThat(mapArguments.get(0).size(), equalTo(1));
        assertThat((String) mapArguments.get(0).get("e"), equalTo("value"));
        assertThat(mapArguments.get(1).size(), equalTo(1));
        assertThat(mapArguments.get(1).containsKey("d"), equalTo(true));
        assertThat(mapArguments.get(2).size(), equalTo(1));
        assertThat(mapArguments.get(2).containsKey("c"), equalTo(true));
        assertThat(mapArguments.get(3).size(), equalTo(1));
        assertThat(mapArguments.get(3).containsKey("b"), equalTo(true));
        assertThat(mapArguments.get(4).size(), equalTo(1));
        assertThat(mapArguments.get(4).containsKey("a"), equalTo(true));
        verify(mockJsonObjectManager).stringify(any(JSONObject.class));
        verifyNoMoreInteractions(mockJsonObjectManager);
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
        serialize(map);

        // Verify the calls.
        verify(mockJsonObjectManager).newObject(mapArgumentCaptor.capture());
        verify(mockJsonObjectManager).newArray(collectionArgumentCaptor.capture());
        Map<String, ?> mapArgument = mapArgumentCaptor.getValue();
        assertThat(mapArgument.size(), equalTo(1));
        assertThat(mapArgument.containsKey("list"), equalTo(true));
        Collection<?> collectionArgument = collectionArgumentCaptor.getValue();
        assertThat(collectionArgument.size(), equalTo(3));
        assertThat(collectionArgument.contains("a"), equalTo(true));
        assertThat(collectionArgument.contains("b"), equalTo(true));
        assertThat(collectionArgument.contains("c"), equalTo(true));
        verify(mockJsonObjectManager).stringify(any(JSONObject.class));
        verifyNoMoreInteractions(mockJsonObjectManager);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void writeMapWithArray() throws Exception {
        // Build a map with a collection.
        Map<String, Object> map = new HashMap<String, Object>();
        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        list.add("c");
        map.put("list", list.toArray(new String[list.size()]));

        // Serialize the map.
        serialize(map);

        // Verify the calls.
        verify(mockJsonObjectManager).newObject(mapArgumentCaptor.capture());
        verify(mockJsonObjectManager).newArray(collectionArgumentCaptor.capture());
        Map<String, ?> mapArgument = mapArgumentCaptor.getValue();
        assertThat(mapArgument.size(), equalTo(1));
        assertThat(mapArgument.containsKey("list"), equalTo(true));
        Collection<?> collectionArgument = collectionArgumentCaptor.getValue();
        assertThat(collectionArgument.size(), equalTo(3));
        assertThat((Collection<String>) collectionArgument, contains("a", "b", "c"));
        verify(mockJsonObjectManager).stringify(any(JSONObject.class));
        verifyNoMoreInteractions(mockJsonObjectManager);
    }

    @Test
    @SuppressWarnings("unchecked")
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

        // {
        //   "a": "1",
        //   "b": "2",
        //   "o": [
        //     "c",
        //     {
        //       "d": "4",
        //       "e": "5",
        //       "m": [
        //         "f",
        //         {
        //           "g": "7"
        //           "j": [
        //             "h",
        //             "i",
        //           ]
        //           "k": "11"
        //         },
        //         "l"
        //       ],
        //       "n": "14"
        //     }
        //   ]
        // }

        // Serialize the map.
        serialize(map1);

        // Verify the calls.
        verify(mockJsonObjectManager, times(3)).newObject(mapArgumentCaptor.capture());
        verify(mockJsonObjectManager, times(3)).newArray(collectionArgumentCaptor.capture());
        List<Map<String, ?>> mapArguments = mapArgumentCaptor.getAllValues();
        assertThat(mapArguments.size(), equalTo(3));
        assertThat(mapArguments.get(0).keySet(), containsInAnyOrder("g", "j", "k"));
        assertThat(mapArguments.get(1).keySet(), containsInAnyOrder("d", "e", "m", "n"));
        assertThat(mapArguments.get(2).keySet(), containsInAnyOrder("a", "b", "o"));
        List<Collection<?>> collectionArguments = collectionArgumentCaptor.getAllValues();
        assertThat(collectionArguments.size(), equalTo(3));
        assertThat((Collection<String>) collectionArguments.get(0), contains("h", "i"));
        assertThat(collectionArguments.get(1).size(), equalTo(3));
        assertThat((Collection<Object>) collectionArguments.get(1), Matchers.<Object>hasItems("f", "l"));
        assertThat(collectionArguments.get(2).size(), equalTo(2));
        assertThat((Collection<Object>) collectionArguments.get(2), Matchers.<Object>hasItem("c"));
        verify(mockJsonObjectManager).stringify(any(JSONObject.class));
        verifyNoMoreInteractions(mockJsonObjectManager);
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
