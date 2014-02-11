package io.keen.client.android;

import android.util.JsonWriter;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import io.keen.client.java.KeenJsonHandler;

/**
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 */
class AndroidStreamingJsonHandler implements KeenJsonHandler {

    private static final DateFormat ISO_8601_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    @Override
    public Map<String, Object> readJson(Reader reader) throws IOException {
        // TODO: Implement reading.
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeJson(Writer writer, Map<String, ?> value) throws IOException {
        JsonWriter jsonWriter = new JsonWriter(writer);
        writeMap(jsonWriter, value);
    }

    private void writeMap(JsonWriter writer, Map<String, ?> map) throws IOException {
        writer.beginObject();
        for (Map.Entry entry : map.entrySet()) {
            // TODO: Make this more type-safe, and generally clean it up.
            writer.name((String) entry.getKey());
            writeValue(writer, entry.getValue());
        }
        writer.endObject();
    }

    private void writeArray(JsonWriter writer, Iterable<?> list) throws IOException {
        writer.beginArray();
        for (Object value : list) {
            writeValue(writer, value);
        }
        writer.endArray();
    }

    @SuppressWarnings("unchecked")
    private void writeValue(JsonWriter writer, Object value) throws IOException {
        // TODO: Handle null, Boolean, Integer, Long, Double
        if (value instanceof Map) {
            writeMap(writer, (Map<String, Object>) value);
        } else if (value instanceof Iterable) {
            writeArray(writer, (Iterable) value);
        } else if (value instanceof Calendar) {
            Date date = ((Calendar) value).getTime();
            String dateString = ISO_8601_FORMAT.format(date);
            writer.value(dateString);
        } else if (value instanceof String) {
            writer.value((String) value);
        } else {
            throw new UnsupportedOperationException("Unsupported value type " +
                    value.getClass().getCanonicalName());
        }
    }

}
