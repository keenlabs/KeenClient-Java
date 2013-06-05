package io.keen.client.java;

import io.keen.client.java.exceptions.ScopedKeyException;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ScopedKeysTests {

    @Test
    public void testEncryptionAndDecryption() throws ScopedKeyException {
        String apiKey = "80ce00d60d6443118017340c42d1cfaf";

        // create the options we'll use
        Map<String, Object> options = new HashMap<String, Object>();
        options.put("allowed_operations", Arrays.asList("read"));
        List<Map<String, Object>> filters = new ArrayList<Map<String, Object>>();
        filters.add(getFilter("purchase.amount", "eq", 56));
        filters.add(getFilter("purchase.name", "ne", "Barbie"));
        options.put("filters", filters);

        // do the encryption
        String scopedKey = ScopedKeys.encrypt(apiKey, options);
        assertTrue(scopedKey.length() > 0);
        System.out.println(scopedKey);

        // now do the decryption
        Map<String, Object> decryptedOptions = ScopedKeys.decrypt(apiKey, scopedKey);

        assertEquals(options, decryptedOptions);
    }

    private Map<String, Object> getFilter(String propertyName, String operator, Object propertyValue) {
        Map<String, Object> filter = new HashMap<String, Object>();
        filter.put("property_name", propertyName);
        filter.put("operator", operator);
        filter.put("property_value", propertyValue);
        return filter;
    }

}
