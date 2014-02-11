package io.keen.client.java;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 */
public interface KeenEventStore {

    OutputStream getCacheOutputStream(String eventCollection) throws IOException;

    CacheEntries retrieveCached() throws IOException;

    void removeFromCache(Object handle) throws IOException;

    public static class CacheEntries {

        public final Map<String, List<Object>> handles;
        public final Map<String, List<Map<String, Object>>> events;

        public CacheEntries(Map<String, List<Object>> handles,
                            Map<String, List<Map<String, Object>>> events) {
            this.handles = handles;
            this.events = events;
        }
    }

}