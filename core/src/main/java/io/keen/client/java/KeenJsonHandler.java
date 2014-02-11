package io.keen.client.java;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Map;

/**
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 */
public interface KeenJsonHandler {

    Map<String, Object> readJson(Reader reader) throws IOException;

    void writeJson(Writer writer, Map<String, ?> value) throws IOException;

}