package io.keen.client.java;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the Keen JSON handler interface using the Jackson JSON library.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class JacksonJsonHandler implements KeenJsonHandler {

    ///// KeenJsonHandler METHODS /////

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> readJson(Reader reader) throws IOException {
        // TODO : We can't assume the top-level node is a JSON Object anymore, because parts of the
        // API we access return a JSON Array as the root. So we need to detect the type and decide
        // what to return, so we need a different return type. Technically it could be a List or
        // Map, so it should be Object, then client code would need to do the instanceof check. For
        // now, so as to not break the KeenJsonHandler interface, we can we can stick a dummy "root"
        // key in the map we pass back.

        // We are expecting this isn't called on nested nodes in a recursive manner.
        JsonNode rootNode = mapper.readTree(reader);
        Map<String, Object> rootMap = null;

        /*
        TODO : Are the calls to traverse() here any better or worse than doing either of these?:

        // Not specifically typed, but our parameter type is Object anyway. This also probably
        // calls traverse() anyway.
        mapper.treeToValue(rootNode, List.class)

        // Precise type information, but potentially creating a new ObjectReader? Also ends up
        // calling traverse() I think.
        mapper.reader(COLLECTION_TYPE).readValue(rootNode)

        // I read this was bad because it's a two-step conversion, but I'm not sure that's true.
        mapper.convertValue(rootNode, COLLECTION_TYPE)
        */

        // If we try to parse an empty string or invalid content, we could get null.
        if (null == rootNode) {
            throw new IllegalArgumentException("Empty reader or ill-formatted JSON encountered.");
        } else if (rootNode.isArray()) {
            rootMap = new LinkedHashMap<String, Object>();
            rootMap.put("io.keen.client.java.__fake_root",
                        mapper.readValue(rootNode.traverse(), COLLECTION_TYPE));
        } else if (rootNode.isObject()) {
            rootMap = mapper.readValue(rootNode.traverse(), MAP_TYPE);
        }

        return rootMap;
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
    public JacksonJsonHandler() {
        mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    ///// PRIVATE CONSTANTS /////

    private static final MapType MAP_TYPE =
            TypeFactory.defaultInstance().constructMapType(Map.class, String.class, Object.class);

    private static final CollectionType COLLECTION_TYPE =
            TypeFactory.defaultInstance().constructCollectionType(List.class, Object.class);

    ///// PRIVATE FIELDS /////

    private final ObjectMapper mapper;

}
