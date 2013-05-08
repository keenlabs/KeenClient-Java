package io.keen.client.java;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Scanner;

/**
 * KeenUtils
 *
 * @author dkador
 * @since 1.0.0
 */
class KeenUtils {

    static String convertStreamToString(java.io.InputStream is) {
        Scanner s = new Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }

    static String getStackTraceFromThrowable(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        return sw.toString(); // stack trace as a string
    }

}
