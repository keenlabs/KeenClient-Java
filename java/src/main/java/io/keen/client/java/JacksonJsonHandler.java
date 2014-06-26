package io.keen.client.java;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Map;

/**
 * Implementation of the Keen JSON handler interface using the Jackson JSON library.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
class JacksonJsonHandler implements KeenJsonHandler {

    ///// KeenJsonHandler METHODS /////

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> readJson(Reader reader) throws IOException {
        return mapper.readValue(reader, MAP_TYPE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeJson(Writer writer, Map<String, ?> value) throws IOException {
        mapper.writeValue(writer, value);
    }

    ///// DEFAULT ACCESS CONSTRUCTORS /////

    /**
     * Constructs a new Jackson JSON handler.
     */
    JacksonJsonHandler() {
        mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    ///// PRIVATE CONSTANTS /////

    private static final MapType MAP_TYPE =
            TypeFactory.defaultInstance().constructMapType(Map.class, String.class, Object.class);

    ///// PRIVATE FIELDS /////

    private final ObjectMapper mapper;

}
