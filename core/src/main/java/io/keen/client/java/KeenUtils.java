package io.keen.client.java;

import java.io.Closeable;
import java.io.IOException;
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

    private final static char[] hexArray = {'0','1','2','3','4','5','6','7',
            '8','9','A','B','C','D','E','F'};

    static void closeQuietly(Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
                // Ignore.
            }
        }
    }

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

    static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        if (len % 2 != 0) {
            throw new IllegalArgumentException("Hex string must have an even length");
        }

        byte[] data = new byte[len / 2];
        for(int i = 0; i < len; i += 2){
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) +
                                   Character.digit(s.charAt(i + 1), 16));
        }

        return data;
    }

    static String byteArrayToHexString(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];

        for(int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xFF;
            hexChars[i * 2] = hexArray[b >>> 4];
            hexChars[(i * 2) + 1] = hexArray[b & 0x0F];
        }

        return new String(hexChars);
    }

}
