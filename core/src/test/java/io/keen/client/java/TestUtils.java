package io.keen.client.java;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * TestUtils
 *
 * @author dkador
 * @since 1.0.0
 */
class TestUtils {
    static String getString(int length) {
        String tooLong = "";
        for (int i = 0; i < length; i++) {
            tooLong += "a";
        }
        return tooLong;
    }

    static Map<String, Object> getSimpleEvent() {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("a", "b");
        return event;
    }

    static void deleteRecursively(File file) {
        if (!file.exists()) {
            return;
        }

        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                deleteRecursively(child);
            }
        }
        if (!file.delete()) {
            throw new RuntimeException("Couldn't delete " + file.getAbsolutePath());
        }
    }

}
