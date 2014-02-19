package io.keen.client.java;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Map;

/**
 * DOCUMENT
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public interface KeenJsonHandler {

    /**
     * DOCUMENT
     *
     * @param reader
     * @return
     * @throws IOException
     */
    Map<String, Object> readJson(Reader reader) throws IOException;

    /**
     * DOCUMENT
     *
     * @param writer
     * @param value
     * @throws IOException
     */
    void writeJson(Writer writer, Map<String, ?> value) throws IOException;

}