package io.keen.client.java;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 */
public class RamEventStore implements KeenEventStore {

    @Override
    public OutputStream getCacheOutputStream(String eventCollection) throws IOException {
        // TODO: Implement this.
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CacheEntries retrieveCached() throws IOException {
        // TODO: Implement this.
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void removeFromCache(Object handle) throws IOException {
        // TODO: Implement this.
        throw new UnsupportedOperationException("Not implemented");
    }

}
