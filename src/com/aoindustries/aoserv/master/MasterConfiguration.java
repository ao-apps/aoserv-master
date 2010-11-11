/*
 * Copyright 2001-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.io.AOPool;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * The configuration for all AOServ processes is stored in a properties file.
 *
 * @author  AO Industries, Inc.
 */
final class MasterConfiguration {
    
    private static Properties props;
    
    private MasterConfiguration() {
    }

    private static String getProperty(String name) throws IOException {
        synchronized (MasterConfiguration.class) {
            if (props == null) {
                Properties newProps = new Properties();
                InputStream in = new BufferedInputStream(MasterConfiguration.class.getResourceAsStream("aoserv-master.properties"));
                try {
                    newProps.load(in);
                } finally {
                    in.close();
                }
                props = newProps;
            }
        }
        return props.getProperty(name);
    }

    static String getDBDriver() throws IOException {
        return getProperty("aoserv.master.db.driver");
    }

    static String getDBURL() throws IOException {
        return getProperty("aoserv.master.db.url");
    }

    static String getDBUser() throws IOException {
        return getProperty("aoserv.master.db.user");
    }

    static String getDBPassword() throws IOException {
        return getProperty("aoserv.master.db.password");
    }

    static int getDBConnectionPoolSize() throws IOException {
        return Integer.parseInt(getProperty("aoserv.master.db.connections"));
    }

    static long getDBMaxConnectionAge() throws IOException {
        String S=getProperty("aoserv.master.db.max_connection_age");
        return S==null || S.length()==0 ? AOPool.DEFAULT_MAX_CONNECTION_AGE : Long.parseLong(S);
    }

    static UserId getRootUsername() throws IOException, ValidationException {
        return UserId.valueOf(getProperty("aoserv.master.root.username"));
    }

    static String getRootPassword() throws IOException {
        return getProperty("aoserv.master.root.password");
    }

    static InetAddress getRmiListenAddress() throws IOException, ValidationException {
        String s = getProperty("aoserv.master.rmi.listen_address");
        return s==null || s.length()==0 ? null : InetAddress.valueOf(s);
    }

    static Hostname getRmiPublicAddress() throws IOException, ValidationException {
        String s = getProperty("aoserv.master.rmi.public_address");
        return s==null || s.length()==0 ? null : Hostname.valueOf(s);
    }

    static boolean getRmiUseSsl() throws IOException {
        return !"false".equals(getProperty("aoserv.master.rmi.useSsl"));
    }

    static NetPort getRmiPort() throws IOException, ValidationException {
        return NetPort.valueOf(Integer.parseInt(getProperty("aoserv.master.rmi.port")));
    }

    static boolean isThreadLocaleEnabled() throws IOException {
        return !"false".equals(getProperty("aoserv.master.threadLocale.enabled"));
    }

    static boolean isTraceEnabled() throws IOException {
        return "true".equals(getProperty("aoserv.master.trace.enabled"));
    }

    /**
     * Gets the local IP address used for outgoing connections to the daemons.
     */
    static InetAddress getLocalIp() throws IOException, ValidationException {
        return InetAddress.valueOf(getProperty("aoserv.master.local_ip"));
    }

    static String getSSLKeystorePassword() throws IOException {
        return getProperty("aoserv.master.ssl.keystore.password");
    }

    static String getSSLKeystorePath() throws IOException {
        return getProperty("aoserv.master.ssl.keystore.path");
    }

    static String getSSLTruststorePassword() throws IOException {
        return getProperty("aoserv.daemon.client.ssl.truststore.password");
    }

    static String getSSLTruststorePath() throws IOException {
        return getProperty("aoserv.daemon.client.ssl.truststore.path");
    }

    static String getDaemonKey(AOServer aoServer) throws IOException {
        return getProperty("aoserv.daemon.client.key."+aoServer.getHostname());
    }
}
