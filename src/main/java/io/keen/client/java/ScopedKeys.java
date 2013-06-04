package io.keen.client.java;

import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.keen.client.java.exceptions.ScopedKeyException;
import org.apache.commons.codec.binary.Hex;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.AlgorithmParameters;
import java.util.HashMap;
import java.util.Map;

/**
 * ScopedKeys is a utility class for dealing with Keen IO Scoped Keys. You'll probably only ever need the
 * encrypt method. However, for completeness, there's also a decrypt method.
 * <p/>
 * Example usage:
 * <p/>
 * <pre>
 *     String apiKey = "YOUR_API_KEY_HERE";
 *     // create the options we'll use
 *     Map<String, Object> options = new HashMap<String, Object>();
 *     options.put("allowed_operations", Arrays.asList("write"));
 *     // do the encryption
 *     String scopedKey = ScopedKeys.encrypt(apiKey, options);
 * </pre>
 *
 * @author dkador
 * @since 1.0.3
 */
public class ScopedKeys {

    private static final int BLOCK_SIZE = 32;

    /**
     * Encrypts the given options with a Keen IO API Key and creates a Scoped Key.
     *
     * @param apiKey  Your Keen IO API Key.
     * @param options The options you want to encrypt.
     * @return A Keen IO Scoped Key.
     * @throws ScopedKeyException
     */
    public static String encrypt(String apiKey, Map<String, Object> options) throws ScopedKeyException {
        try {
            // if the user doesn't give an options, just use an empty one
            if (options == null) {
                options = new HashMap<String, Object>();
            }

            // pad the api key
            final String paddedApiKey = padApiKey(apiKey);

            // json encode the options
            final String jsonOptions = KeenClient.MAPPER.writeValueAsString(options);

            // pad the options
            final String paddedJsonOptions = pad(jsonOptions);

            // setup the API key as the secret
            final SecretKey secret = new SecretKeySpec(paddedApiKey.getBytes("UTF-8"), "AES");

            // get the right AES cipher
            final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, secret);

            final AlgorithmParameters params = cipher.getParameters();
            // get a random IV for each encryption
            final byte[] iv = params.getParameterSpec(IvParameterSpec.class).getIV();
            // do the actual encryption
            final byte[] cipherText = cipher.doFinal(paddedJsonOptions.getBytes("UTF-8"));

            // now return the hexed iv + the hexed cipher text
            return new String(Hex.encodeHex(iv)) + new String(Hex.encodeHex(cipherText));
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
     * @throws ScopedKeyException
     */
    public static Map<String, Object> decrypt(String apiKey, String scopedKey) throws ScopedKeyException {
        try {
            // pad the api key
            final String paddedApiKey = padApiKey(apiKey);

            // grab first 16 bytes (aka 32 characters of hex) - that's the IV
            String hexedIv = scopedKey.substring(0, 32);

            // grab everything else - that's the ciphertext (aka encrypted message)
            String hexedCipherText = scopedKey.substring(32);

            // unhex the iv and ciphertext
            byte[] iv = Hex.decodeHex(hexedIv.toCharArray());
            byte[] cipherText = Hex.decodeHex(hexedCipherText.toCharArray());

            // setup the API key as the secret
            final SecretKey secret = new SecretKeySpec(paddedApiKey.getBytes("UTF-8"), "AES");

            // get the right AES cipher
            final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");

            // initialize the cipher with the right IV
            IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);
            cipher.init(Cipher.DECRYPT_MODE, secret, ivParameterSpec);

            // do the decryption
            String plainText = new String(cipher.doFinal(cipherText), "UTF-8");

            // unPad the plain text
            String unPaddedPlainText = unPad(plainText);

            // do this to deal with type erasure
            MapType javaType = TypeFactory.defaultInstance().constructMapType(Map.class, String.class, Object.class);

            // return the JSON decoded options map
            return KeenClient.MAPPER.readValue(unPaddedPlainText, javaType);
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

    private static String unPad(String input) {
        int paddingSize = input.charAt(input.length() - 1);
        return input.substring(0, input.length() - paddingSize);
    }

}
