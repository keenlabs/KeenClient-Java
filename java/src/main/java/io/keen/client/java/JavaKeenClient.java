package io.keen.client.java;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * TODO: Add documentation.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class JavaKeenClient {

    public static void initialize() {
        KeenClient.initialize();

        KeenClient.KeenClientInterfaces interfaces = new KeenClient.KeenClientInterfaces();
        interfaces.eventStore = null;
        interfaces.jsonHandler = new JacksonJsonHandler();
        interfaces.publishExecutor = Executors.newFixedThreadPool(KeenConfig.NUM_THREADS_FOR_HTTP_REQUESTS);
        KeenClient.interfaces = interfaces;
    }

    public static KeenClient client() {
        return KeenClient.client();
    }

    private static final class JacksonJsonHandler implements KeenClient.KeenJsonHandler {

        private static final MapType MAP_TYPE =
                TypeFactory.defaultInstance().constructMapType(Map.class, String.class, Object.class);

        private final ObjectMapper mapper;

        JacksonJsonHandler() {
            mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        }

        @Override
        public Map<String, Object> readJson(Reader reader) throws IOException {
            return mapper.readValue(reader, MAP_TYPE);
        }

        @Override
        public void writeJson(Writer writer, Map<String, ? extends Object> value) throws IOException {
            mapper.writeValue(writer, value);
        }
    }

}
