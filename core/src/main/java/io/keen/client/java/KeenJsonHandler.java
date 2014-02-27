package io.keen.client.java;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Map;

/**
 * Interface for abstracting the tasks of converting an input {@link java.io.Reader} into an
 * in-memory object (in the form of a {@code Map&lt;String, Object&gt;}), and for writing that
 * object back out to a {@link java.io.Writer}.
 * <p/>
 * This interface allows the Keen library to be configured to use different JSON implementations
 * in different environments, depending upon requirements for speed versus size (or other
 * considerations).
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public interface KeenJsonHandler {

    /**
     * Reads JSON-formatted data from the provided {@link java.io.Reader} and constructs a
     * {@link java.util.Map} representing the object described. The keys of the map should
     * correspond to the names of the top-level members, and the values may primitives (Strings,
     * Integers, Booleans, etc.), Maps, or Iterables.
     *
     * @param reader The {@link java.io.Reader} from which to read the JSON data.
     * @return The object which was read, held in a {@code Map&lt;String, Object&gt;}.
     * @throws IOException If there is an error reading from the input.
     */
    Map<String, Object> readJson(Reader reader) throws IOException;

    /**
     * Writes the given object (in the form of a {@code Map&lt;String, Object&gt;} to the specified
     * {@link java.io.Writer}.
     *
     * @param writer The {@link java.io.Writer} to which the JSON data should be written.
     * @param value  The object to write.
     * @throws IOException If there is an error writing to the output.
     */
    void writeJson(Writer writer, Map<String, ?> value) throws IOException;

}