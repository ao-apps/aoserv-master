package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 2200 Dogwood Ct N, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.io.*;
import com.aoindustries.profiler.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

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
        Profiler.startProfile(Profiler.IO, MasterConfiguration.class, "getProperty(String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }
    
    public static String getErrorSmtpServer() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getErrorSmtpServer()", null);
        try {
            return getProperty("aoserv.master.error.smtp.server");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getWarningSmtpServer() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getWarningSmtpServer()", null);
        try {
            return getProperty("aoserv.master.warning.smtp.server");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getErrorEmailFrom() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getErrorEmailFrom()", null);
        try {
            return getProperty("aoserv.master.error.email.from");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getWarningEmailFrom() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getWarningEmailFrom()", null);
        try {
            return getProperty("aoserv.master.warning.email.from");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getErrorEmailTo() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getErrorEmailTo()", null);
        try {
            return getProperty("aoserv.master.error.email.to");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    public static String getWarningEmailTo() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getWarningEmailTo()", null);
        try {
            return getProperty("aoserv.master.warning.email.to");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getSecurityEmailFrom() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getSecurityEmailFrom()", null);
        try {
            return getProperty("aoserv.master.security.email.from");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getSecurityEmailTo() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getSecurityEmailTo()", null);
        try {
            return getProperty("aoserv.master.security.email.to");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getSSLKeystorePassword() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getSSLKeystorePassword()", null);
        try {
            return getProperty("aoserv.master.ssl.keystore.password");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getSSLKeystorePath() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getSSLKeystorePath()", null);
        try {
            return getProperty("aoserv.master.ssl.keystore.path");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getSSLTruststorePassword() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getSSLTruststorePassword()", null);
        try {
            return getProperty("aoserv.daemon.client.ssl.truststore.password");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getSSLTruststorePath() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getSSLTruststorePath()", null);
        try {
            return getProperty("aoserv.daemon.client.ssl.truststore.path");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static List<String> getProtocols() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getProtocols()", null);
        try {
            return StringUtility.splitStringCommaSpace(getProperty("aoserv.master.protocols"));
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static List<String> getBinds(String protocol) throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getBinds(String)", null);
        try {
            return StringUtility.splitStringCommaSpace(getProperty("aoserv.master."+protocol+".bind"));
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static int getPort(String protocol) throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getPort(String)", null);
        try {
            return Integer.parseInt(getProperty("aoserv.master."+protocol+".port"));
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static int getHistorySize() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getHistorySize()", null);
        try {
            return Integer.parseInt(getProperty("aoserv.master.history.size"));
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getRootBusiness() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getRootBusiness()", null);
        try {
            return getProperty("aoserv.master.businesses.root");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getDBDriver() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getDBDriver()", null);
        try {
            return getProperty("aoserv.master.db.driver");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getDBURL() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getDBURL()", null);
        try {
            return getProperty("aoserv.master.db.url");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getDBUser() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getDBUser()", null);
        try {
            return getProperty("aoserv.master.db.user");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getDBPassword() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getDBPassword()", null);
        try {
            return getProperty("aoserv.master.db.password");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static int getDBConnectionPoolSize() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getDBConnectionPoolSize()", null);
        try {
            return Integer.parseInt(getProperty("aoserv.master.db.connections"));
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static long getDBMaxConnectionAge() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getDBMaxConnectionAge()", null);
        try {
            String S=getProperty("aoserv.master.db.max_connection_age");
            return S==null || S.length()==0 ? AOPool.DEFAULT_MAX_CONNECTION_AGE : Long.parseLong(S);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getBackupDBDriver() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getBackupDBDriver()", null);
        try {
            return getProperty("aoserv.master.backup.db.driver");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getBackupDBURL() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getBackupDBURL()", null);
        try {
            return getProperty("aoserv.master.backup.db.url");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getBackupDBUser() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getBackupDBUser()", null);
        try {
            return getProperty("aoserv.master.backup.db.user");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getBackupDBPassword() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getBackupDBPassword()", null);
        try {
            return getProperty("aoserv.master.backup.db.password");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static int getBackupDBConnectionPoolSize() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getBackupDBConnectionPoolSize()", null);
        try {
            return Integer.parseInt(getProperty("aoserv.master.backup.db.connections"));
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static long getBackupDBMaxConnectionAge() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getBackupDBMaxConnectionAge()", null);
        try {
            String S=getProperty("aoserv.master.backup.db.max_connection_age");
            return S==null || S.length()==0 ? AOPool.DEFAULT_MAX_CONNECTION_AGE : Long.parseLong(S);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getDaemonKey(MasterDatabaseConnection conn, int aoServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getDaemonKey(MasterDatabaseConnection,int)", null);
        try {
            return getProperty("aoserv.daemon.client.key."+ServerHandler.getHostnameForServer(conn, aoServer));
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static int getProfilerLevel() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getProfilerLevel()", null);
        try {
            return Profiler.parseProfilerLevel(getProperty("aoserv.master.profiler.level"));
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getTicketSmtpServer() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getTicketSmtpServer()", null);
        try {
            return getProperty("aoserv.master.ticket.smtp.server");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getTicketSource(String protocol, int index, String field) throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getTicketSource(String,int,String)", null);
        try {
            return getProperty("aoserv.master.ticket.source."+protocol+"."+index+"."+field);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getTicketURL() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getTicketURL()", null);
        try {
            return getProperty("aoserv.master.ticket.url");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getSpamhausScriptPath() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getSpamhausScriptPath()", null);
        try {
            return getProperty("aoserv.master.blacklist.spamhaus.script.path");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getDsblCachePath() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getDsblCachePath()", null);
        try {
            return getProperty("aoserv.master.blacklist.dsbl.cache.path");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getDsblScriptPath() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getDsblCachePath()", null);
        try {
            return getProperty("aoserv.master.blacklist.dsbl.script.path");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    public static String getEntropyPoolFilePath() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterConfiguration.class, "getEntropyPoolFilePath()", null);
        try {
            return getProperty("aoserv.master.entropy.file.path");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
}
