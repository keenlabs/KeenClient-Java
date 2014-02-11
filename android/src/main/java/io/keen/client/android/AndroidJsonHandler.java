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
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 */
class AndroidJsonHandler implements KeenJsonHandler {

    private static final int COPY_BUFFER_SIZE = 4 * 1024;

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

    @Override
    public void writeJson(Writer writer, Map<String, ? extends Object> value) throws IOException {
        JSONObject jsonObject = new JSONObject(value);
        writer.write(jsonObject.toString());
    }

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
