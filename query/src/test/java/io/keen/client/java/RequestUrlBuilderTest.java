package io.keen.client.java;

import org.junit.Test;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertThat;

public class RequestUrlBuilderTest {

    private static final String API_VERSION = "v1";
    private static final String BASE_URL = "https://api.keen.io";
    private static final String PROJECT_ID = "0123456789abcdef";
    private static final String DATASET = "testing-things";

    private final RequestUrlBuilder builder = new RequestUrlBuilder(API_VERSION, BASE_URL);

    @Test
    public void shouldCreateBasicDatasetUrl() {
        URL result = builder.getDatasetsUrl(PROJECT_ID, null, false, Collections.<String, Object>emptyMap());

        assertThat(result.toString(), equalTo("https://api.keen.io/v1/projects/0123456789abcdef/datasets"));
    }

    @Test
    public void shouldCreateSpecificDatasetUrl() {
        URL result = builder.getDatasetsUrl(PROJECT_ID, DATASET, false, Collections.<String, Object>emptyMap());

        assertThat(result.toString(), equalTo("https://api.keen.io/v1/projects/0123456789abcdef/datasets/testing-things"));
    }

    @Test
    public void shouldCreateSpecificDatasetResultUrl() {
        URL result = builder.getDatasetsUrl(PROJECT_ID, DATASET, true, Collections.<String, Object>emptyMap());

        assertThat(result.toString(), equalTo("https://api.keen.io/v1/projects/0123456789abcdef/datasets/testing-things/results"));
    }

    @Test
    public void shouldAppendEncodedQueryParams() {
        Map<String, Object> queryParams = new HashMap<String, Object>() {{
            put("key1", "value");
            put("key2", "value?& <3");
            put("key3", "¯\\_(ツ)_/¯");
        }};
        String encodedEntry1 = "key1=value";
        String encodedEntry2 = "key2=value%3F%26+%3C3";
        String encodedEntry3 = "key3=%C2%AF%5C_%28%E3%83%84%29_%2F%C2%AF";

        URL result = builder.getDatasetsUrl(PROJECT_ID, DATASET, true, queryParams);

        assertThat(result.getQuery().length(), equalTo(encodedEntry1.length() + encodedEntry2.length() + encodedEntry3.length() + 2));
        assertThat(result.getQuery(), containsString(encodedEntry1));
        assertThat(result.getQuery(), containsString(encodedEntry2));
        assertThat(result.getQuery(), containsString(encodedEntry3));
    }

}
