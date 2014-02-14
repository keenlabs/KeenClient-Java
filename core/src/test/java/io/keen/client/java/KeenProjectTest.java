package io.keen.client.java;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * RamEventStoreTest
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class KeenProjectTest {

    @Test
    public void constructWithEnvironment() {
        KeenProject project = new KeenProject(getEnvironment("project_id", "abc", "def"));
        doProjectAssertions("project_id", "abc", "def", project);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyEnvironment() {
        new KeenProject();
        fail("Shouldn't be able to get client if no environment set.");
    }

    @Test(expected = IllegalArgumentException.class)
    public void environmentWithNoSettings() {
        new KeenProject(getEnvironment(null, null, null));
        fail("Shouldn't be able to get client if bad environment used.");
    }

    @Test(expected = IllegalArgumentException.class)
    public void environmentWithNoProjectId() {
        new KeenProject(getEnvironment(null, "abc", "def"));
        fail("Shouldn't be able to get client if no project id in environment.");
    }

    private Environment getEnvironment(final String projectId, final String writeKey, final String readKey) {
        return new Environment() {
            @Override
            public String getKeenProjectId() {
                return projectId;
            }

            @Override
            public String getKeenWriteKey() {
                return writeKey;
            }

            @Override
            public String getKeenReadKey() {
                return readKey;
            }
        };
    }

    private void doProjectAssertions(String expectedProjectId, String expectedWriteKey,
                                    String expectedReadKey, KeenProject project) {
        assertEquals(expectedProjectId, project.getProjectId());
        assertEquals(expectedWriteKey, project.getWriteKey());
        assertEquals(expectedReadKey, project.getReadKey());
    }

}
