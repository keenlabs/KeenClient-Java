package io.keen.client.android;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;

import io.keen.client.java.KeenJsonHandler;

/**
 * DOCUMENT
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
class AndroidJsonHandler implements KeenJsonHandler {

    ///// KeenJsonHandler METHODS /////

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> readJson(Reader reader) throws IOException {
        String json = readerToString(reader);
        try {
            JSONObject jsonObject = new JSONObject(json);
            return JsonHelper.toMap(jsonObject);
        } catch (JSONException e) {
            throw new IOException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeJson(Writer writer, Map<String, ? extends Object> value) throws IOException {
        JSONObject jsonObject = new JSONObject(value);
        writer.write(jsonObject.toString());
    }

    ///// PRIVATE CONSTANTS /////

    /** The size of the buffer to use when copying a reader to a string. */
    private static final int COPY_BUFFER_SIZE = 4 * 1024;

    ///// PRIVATE METHODS /////

    /**
     * DOCUMENT
     *
     * @param reader
     * @return
     * @throws IOException
     */
    private static String readerToString(Reader reader) throws IOException {
        StringWriter writer = new StringWriter();
        char[] buffer = new char[COPY_BUFFER_SIZE];
        while (true) {
            int bytesRead = reader.read(buffer);
            if (bytesRead == -1) {
                break;
            } else {
                writer.write(buffer, 0, bytesRead);
            }
        }
        return writer.toString();
    }

}
