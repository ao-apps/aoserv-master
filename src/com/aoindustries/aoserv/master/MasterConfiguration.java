package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.io.AOPool;
import com.aoindustries.profiler.Profiler;
import com.aoindustries.util.StringUtility;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * The configuration for all AOServ processes is stored in a properties file.
 *
 * @author  AO Industries, Inc.
 */
public final class MasterConfiguration {
    
    private static Properties props;
    
    private MasterConfiguration() {
    }
    
    private static String getProperty(String name) throws IOException {
        if (props == null) {
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
        }
        return props.getProperty(name);
    }
    
    public static String getErrorSmtpServer() throws IOException {
        return getProperty("aoserv.master.error.smtp.server");
    }

    public static String getWarningSmtpServer() throws IOException {
        return getProperty("aoserv.master.warning.smtp.server");
    }

    public static String getErrorEmailFrom() throws IOException {
        return getProperty("aoserv.master.error.email.from");
    }

    public static String getWarningEmailFrom() throws IOException {
        return getProperty("aoserv.master.warning.email.from");
    }

    public static String getErrorEmailTo() throws IOException {
        return getProperty("aoserv.master.error.email.to");
    }
    
    public static String getWarningEmailTo() throws IOException {
        return getProperty("aoserv.master.warning.email.to");
    }

    public static String getSecurityEmailFrom() throws IOException {
        return getProperty("aoserv.master.security.email.from");
    }

    public static String getSecurityEmailTo() throws IOException {
        return getProperty("aoserv.master.security.email.to");
    }

    public static String getSSLKeystorePassword() throws IOException {
        return getProperty("aoserv.master.ssl.keystore.password");
    }

    public static String getSSLKeystorePath() throws IOException {
        return getProperty("aoserv.master.ssl.keystore.path");
    }

    public static String getSSLTruststorePassword() throws IOException {
        return getProperty("aoserv.daemon.client.ssl.truststore.password");
    }

    public static String getSSLTruststorePath() throws IOException {
        return getProperty("aoserv.daemon.client.ssl.truststore.path");
    }

    public static List<String> getProtocols() throws IOException {
        return StringUtility.splitStringCommaSpace(getProperty("aoserv.master.protocols"));
    }

    /**
     * Gets the local IP address used for outgoing connections to the daemons.
     */
    public static String getLocalIp() throws IOException {
        return getProperty("aoserv.master.local_ip");
    }

    public static List<String> getBinds(String protocol) throws IOException {
        return StringUtility.splitStringCommaSpace(getProperty("aoserv.master."+protocol+".bind"));
    }

    public static int getPort(String protocol) throws IOException {
        return Integer.parseInt(getProperty("aoserv.master."+protocol+".port"));
    }

    public static int getHistorySize() throws IOException {
        return Integer.parseInt(getProperty("aoserv.master.history.size"));
    }

    public static String getRootBusiness() throws IOException {
        return getProperty("aoserv.master.businesses.root");
    }

    public static String getDBDriver() throws IOException {
        return getProperty("aoserv.master.db.driver");
    }

    public static String getDBURL() throws IOException {
        return getProperty("aoserv.master.db.url");
    }

    public static String getDBUser() throws IOException {
        return getProperty("aoserv.master.db.user");
    }

    public static String getDBPassword() throws IOException {
        return getProperty("aoserv.master.db.password");
    }

    public static int getDBConnectionPoolSize() throws IOException {
        return Integer.parseInt(getProperty("aoserv.master.db.connections"));
    }

    public static long getDBMaxConnectionAge() throws IOException {
        String S=getProperty("aoserv.master.db.max_connection_age");
        return S==null || S.length()==0 ? AOPool.DEFAULT_MAX_CONNECTION_AGE : Long.parseLong(S);
    }

    public static String getBackupDBDriver() throws IOException {
        return getProperty("aoserv.master.backup.db.driver");
    }

    public static String getBackupDBURL() throws IOException {
        return getProperty("aoserv.master.backup.db.url");
    }

    public static String getBackupDBUser() throws IOException {
        return getProperty("aoserv.master.backup.db.user");
    }

    public static String getBackupDBPassword() throws IOException {
        return getProperty("aoserv.master.backup.db.password");
    }

    public static int getBackupDBConnectionPoolSize() throws IOException {
        return Integer.parseInt(getProperty("aoserv.master.backup.db.connections"));
    }

    public static long getBackupDBMaxConnectionAge() throws IOException {
        String S=getProperty("aoserv.master.backup.db.max_connection_age");
        return S==null || S.length()==0 ? AOPool.DEFAULT_MAX_CONNECTION_AGE : Long.parseLong(S);
    }

    public static String getDaemonKey(MasterDatabaseConnection conn, int aoServer) throws IOException, SQLException {
        return getProperty("aoserv.daemon.client.key."+ServerHandler.getHostnameForServer(conn, aoServer));
    }

    public static int getProfilerLevel() throws IOException {
        return Profiler.parseProfilerLevel(getProperty("aoserv.master.profiler.level"));
    }

    public static String getTicketSmtpServer() throws IOException {
        return getProperty("aoserv.master.ticket.smtp.server");
    }

    public static String getTicketSource(String protocol, int index, String field) throws IOException {
        return getProperty("aoserv.master.ticket.source."+protocol+"."+index+"."+field);
    }

    public static String getTicketURL() throws IOException {
        return getProperty("aoserv.master.ticket.url");
    }

    public static String getSpamhausScriptPath() throws IOException {
        return getProperty("aoserv.master.blacklist.spamhaus.script.path");
    }

    public static String getDsblCachePath() throws IOException {
        return getProperty("aoserv.master.blacklist.dsbl.cache.path");
    }

    public static String getDsblScriptPath() throws IOException {
        return getProperty("aoserv.master.blacklist.dsbl.script.path");
    }
    
    public static String getEntropyPoolFilePath() throws IOException {
        return getProperty("aoserv.master.entropy.file.path");
    }
}
