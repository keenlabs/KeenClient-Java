package io.keen.client.java;

import java.io.StringReader;
import java.io.StringWriter;
import java.security.AlgorithmParameters;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import io.keen.client.java.exceptions.ScopedKeyException;

/**
 * ScopedKeys is a utility class for dealing with Keen IO Scoped Keys. You'll probably only ever need the
 * encrypt method. However, for completeness, there's also a decrypt method.
 * <p>
 * Example usage:
 * </p>
 * <pre>
 *     {@code
 *     String apiKey = "YOUR_API_KEY_HERE";
 *
 *     //Filters to apply to the key
 *     Map<String, Object> filter = new HashMap<String, Object>();
 *     List<Map<String, Object>> filters = new ArrayList<Map<String, Object>>();
 *
 *     //Create and add a filter
 *     filter.put("property_name", "user_id");
 *     filter.put("operator", "eq");
 *     filter.put("property_value", "123");
 *
 *     filters.add(filter);
 *
 *     // create the options we'll use
 *     Map<String, Object> options = new HashMap<String, Object>();
 *     options.put("allowed_operations", Arrays.asList("write"));
 *     options.put("filters", filters);
 *
 *     // do the encryption
 *     String scopedKey = ScopedKeys.encrypt(apiKey, options);
 *     }
 * </pre>
 *
 * @author dkador
 * @since 1.0.3
 */
public class ScopedKeys {

    private static final int BLOCK_SIZE = 32;

    // TODO: Review exceptions from this class.

    /**
     * Encrypts the given options with a Keen IO API Key and creates a Scoped Key.
     *
     * @param apiKey  Your Keen IO API Key.
     * @param options The options you want to encrypt.
     * @return A Keen IO Scoped Key.
     * @throws ScopedKeyException an error occurred while attempting to encrypt a Scoped Key.
     */
    public static String encrypt(String apiKey, Map<String, Object> options)
            throws ScopedKeyException {
        return encrypt(KeenClient.client(), apiKey, options);
    }

    /**
     * Encrypts the given options with a Keen IO API Key and creates a Scoped Key.
     *
     * @param client  The KeenClient to use for JSON handling.
     * @param apiKey  Your Keen IO API Key.
     * @param options The options you want to encrypt.
     * @return A Keen IO Scoped Key.
     * @throws ScopedKeyException an error occurred while attempting to encrypt a Scoped Key.
     */
    public static String encrypt(KeenClient client, String apiKey, Map<String, Object> options)
        throws ScopedKeyException {
        try {
            // if the user doesn't give an options, just use an empty one
            if (options == null) {
                options = new HashMap<String, Object>();
            }

            // pad the api key
            final String paddedApiKey = padApiKey(apiKey);

            // json encode the options
            StringWriter writer = new StringWriter();
            client.getJsonHandler().writeJson(writer, options);
            final String jsonOptions = writer.toString();

            // setup the API key as the secret
            final SecretKey secret = new SecretKeySpec(paddedApiKey.getBytes("UTF-8"), "AES");

            // get the right AES cipher
            final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, secret);

            final AlgorithmParameters params = cipher.getParameters();
            // get a random IV for each encryption
            final byte[] iv = params.getParameterSpec(IvParameterSpec.class).getIV();
            // do the actual encryption (this also pads jsonOptions)
            final byte[] cipherText = cipher.doFinal(jsonOptions.getBytes("UTF-8"));

            // now return the hexed iv + the hexed cipher text
            return KeenUtils.byteArrayToHexString(iv) + KeenUtils.byteArrayToHexString(cipherText);
        } catch (Exception e) {
            throw new ScopedKeyException("An error occurred while attempting to encrypt a Scoped Key", e);
        }
    }

    /**
     * Decrypts the given Keen IO Scoped Key with an API Key and returns the decrypted Scoped Key Options.
     *
     * @param apiKey    Your Keen IO API Key.
     * @param scopedKey The Scoped Key you want to decrypt.
     * @return The decrypted Scoped Key Options.
     * @throws ScopedKeyException an error occurred while attempting to decrypt a Scoped Key.
     */
    public static Map<String, Object> decrypt(String apiKey, String scopedKey)
            throws ScopedKeyException {
        return decrypt(KeenClient.client(), apiKey, scopedKey);
    }

    /**
     * Decrypts the given Keen IO Scoped Key with an API Key and returns the decrypted Scoped Key Options.
     *
     * @param client  The KeenClient to use for JSON handling.
     * @param apiKey    Your Keen IO API Key.
     * @param scopedKey The Scoped Key you want to decrypt.
     * @return The decrypted Scoped Key Options.
     * @throws ScopedKeyException an error occurred while attempting to decrypt a Scoped Key.
     */
    public static Map<String, Object> decrypt(KeenClient client, String apiKey, String scopedKey)
        throws ScopedKeyException {
        try {
            // pad the api key
            final String paddedApiKey = padApiKey(apiKey);

            // grab first 16 bytes (aka 32 characters of hex) - that's the IV
            String hexedIv = scopedKey.substring(0, 32);

            // grab everything else - that's the ciphertext (aka encrypted message)
            String hexedCipherText = scopedKey.substring(32);

            // unhex the iv and ciphertext
            byte[] iv = KeenUtils.hexStringToByteArray(hexedIv);
            byte[] cipherText = KeenUtils.hexStringToByteArray(hexedCipherText);

            // setup the API key as the secret
            final SecretKey secret = new SecretKeySpec(paddedApiKey.getBytes("UTF-8"), "AES");

            // get the right AES cipher
            final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");

            // initialize the cipher with the right IV
            IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);
            cipher.init(Cipher.DECRYPT_MODE, secret, ivParameterSpec);

            // do the decryption
            String plainText = new String(cipher.doFinal(cipherText), "UTF-8");

            // return the JSON decoded options map
            return client.getJsonHandler().readJson(new StringReader(plainText));
        } catch (Exception e) {
            throw new ScopedKeyException("An error occurred while attempting to decrypt a Scoped Key", e);
        }
    }

    private static String padApiKey(String apiKey) {
        if (apiKey.length() % BLOCK_SIZE == 0) {
            return apiKey; // don't have to do anything if we're already at the block size
        } else {
            return pad(apiKey);
        }
    }

    private static String pad(String input) {
        // if the last block is already full, add another one
        final int paddingSize = BLOCK_SIZE - (input.length() % BLOCK_SIZE);

        String padding = "";
        for (int i = 0; i < paddingSize; i++) {
            padding += (char) paddingSize;
        }

        return input + padding;
    }

}
