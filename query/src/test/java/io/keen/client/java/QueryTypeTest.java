package io.keen.client.java;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class QueryTypeTest {

    @Test
    public void shouldCreateQueryTypeBasingOnString() {
        for (QueryType value : QueryType.values()) {
            assertThat(QueryType.valueOfIgnoreCase(value.toString()), equalTo(value));
            assertThat(QueryType.valueOfIgnoreCase(value.toString().toLowerCase()), equalTo(value));
        }
    }
}
