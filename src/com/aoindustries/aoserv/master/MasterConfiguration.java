package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.validator.Hostname;
import com.aoindustries.aoserv.client.validator.InetAddress;
import com.aoindustries.aoserv.client.validator.NetPort;
import com.aoindustries.aoserv.client.validator.ValidationException;
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
    
    static String getSSLKeystorePassword() throws IOException {
        return getProperty("aoserv.master.ssl.keystore.password");
    }

    static String getSSLKeystorePath() throws IOException {
        return getProperty("aoserv.master.ssl.keystore.path");
    }

    /* TODO
    static String getSSLTruststorePassword() throws IOException {
        return getProperty("aoserv.daemon.client.ssl.truststore.password");
    }

    static String getSSLTruststorePath() throws IOException {
        return getProperty("aoserv.daemon.client.ssl.truststore.path");
    }

    static List<String> getProtocols() throws IOException {
        return StringUtility.splitStringCommaSpace(getProperty("aoserv.master.protocols"));
    }
     */

    /**
     * Gets the local IP address used for outgoing connections to the daemons.
     */
    /* TODO
    static String getLocalIp() throws IOException {
        return getProperty("aoserv.master.local_ip");
    }

    static List<String> getBinds(String protocol) throws IOException {
        return StringUtility.splitStringCommaSpace(getProperty("aoserv.master."+protocol+".bind"));
    }

    static List<Integer> getPorts(String protocol) throws IOException {
        String ports = getProperty("aoserv.master."+protocol+".ports");
        List<String> strings = StringUtility.splitStringCommaSpace(ports);
        List<Integer> ints = new ArrayList<Integer>(strings.size());
        for(int c=0,len=strings.size();c<len;c++) {
            ints.add(Integer.parseInt(strings.get(c)));
        }
        return ints;
    }

    static int getHistorySize() throws IOException {
        return Integer.parseInt(getProperty("aoserv.master.history.size"));
    }

    static String getRootBusiness() throws IOException {
        return getProperty("aoserv.master.businesses.root");
    }
    */
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
    /* TODO
    static String getBackupDBDriver() throws IOException {
        return getProperty("aoserv.master.backup.db.driver");
    }

    static String getBackupDBURL() throws IOException {
        return getProperty("aoserv.master.backup.db.url");
    }

    static String getBackupDBUser() throws IOException {
        return getProperty("aoserv.master.backup.db.user");
    }

    static String getBackupDBPassword() throws IOException {
        return getProperty("aoserv.master.backup.db.password");
    }

    static int getBackupDBConnectionPoolSize() throws IOException {
        return Integer.parseInt(getProperty("aoserv.master.backup.db.connections"));
    }

    static long getBackupDBMaxConnectionAge() throws IOException {
        String S=getProperty("aoserv.master.backup.db.max_connection_age");
        return S==null || S.length()==0 ? AOPool.DEFAULT_MAX_CONNECTION_AGE : Long.parseLong(S);
    }

    static String getDaemonKey(DatabaseAccess database, int aoServer) throws IOException, SQLException {
        return getProperty("aoserv.daemon.client.key."+ServerHandler.getHostnameForAOServer(database, aoServer));
    }

    static int getProfilerLevel() throws IOException {
        return Profiler.parseProfilerLevel(getProperty("aoserv.master.profiler.level"));
    }

    static String getTicketSmtpServer() throws IOException {
        return getProperty("aoserv.master.ticket.smtp.server");
    }

    static String getTicketSource(String protocol, int index, String field) throws IOException {
        return getProperty("aoserv.master.ticket.source."+protocol+"."+index+"."+field);
    }

    static String getTicketURL() throws IOException {
        return getProperty("aoserv.master.ticket.url");
    }

    static String getSpamhausScriptPath() throws IOException {
        return getProperty("aoserv.master.blacklist.spamhaus.script.path");
    }

    static String getDsblCachePath() throws IOException {
        return getProperty("aoserv.master.blacklist.dsbl.cache.path");
    }

    static String getDsblScriptPath() throws IOException {
        return getProperty("aoserv.master.blacklist.dsbl.script.path");
    }
    
    static String getEntropyPoolFilePath() throws IOException {
        return getProperty("aoserv.master.entropy.file.path");
    }
    */

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
}
