package io.keen.client.java;

import java.io.IOException;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * {@link io.keen.client.java.KeenClient.Builder} with defaults suited for use in a standard Java
 * environment.
 * <p/>
 * This client uses the Jackson library for reading and writing JSON. As a result, Jackson must be
 * available in order for this library to work properly. For applications which would prefer to
 * use a different JSON library, configure the builder to use an appropriate {@link KeenJsonHandler}
 * via the {@link #withJsonHandler(KeenJsonHandler)} method.
 * <p/>
 * Other defaults are those provided by the parent {@link KeenClient.Builder} implementation.
 * <p/>
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class JavaKeenClientBuilder extends KeenClient.Builder {

    private String networkInterfaceName;

    public JavaKeenClientBuilder setNetworkInterfaceName(String ifname) {
        networkInterfaceName = ifname;
        return this;
    }

    @Override
    protected KeenJsonHandler getDefaultJsonHandler() {
        return new JacksonJsonHandler();
    }

    @Override
    public boolean isNetworkConnected() {
        // if no network interface is specified, skip this check
        if (networkInterfaceName == null) {
            return true;
        }

        try {
            // loop through the interfaces, looking for the one that matches
            // `networkInterfaceName`.
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (!networkInterfaceName.equals(networkInterface.getDisplayName())) {
                    continue;
                }

                // if we find a network interface matching
                // `networkInterfaceName`, check if it is up.
                if (networkInterface.isUp()) {
                    return true;
                }
            }
        } catch(Exception e) {
            // quietly fail
        }

        // if we get here, we didn't find a network interface with a matching
        // name, or if we did find one, it wasn't up.
        return false;
    }

}
