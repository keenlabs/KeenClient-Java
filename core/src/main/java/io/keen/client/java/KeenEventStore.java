package io.keen.client.java;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * DOCUMENT
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public interface KeenEventStore {

    /**
     * DOCUMENT
     *
     * @param eventCollection
     * @param event
     * @return
     * @throws IOException
     */
    Object store(String eventCollection, Map<String, Object> event) throws IOException;

    /**
     * DOCUMENT
     *
     * @param handle
     * @return
     * @throws IOException
     */
    Map<String, Object> get(Object handle) throws IOException;

    /**
     * DOCUMENT
     *
     * @param handle
     * @throws IOException
     */
    void remove(Object handle) throws IOException;

    /**
     * DOCUMENT
     *
     * @return
     * @throws IOException
     */
    Map<String, List<Object>> getHandles() throws IOException;

}