/*
 * Copyright 2001-2013 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.InetAddress;
import com.aoindustries.aoserv.client.validator.ValidationException;
import com.aoindustries.io.AOPool;
import com.aoindustries.profiler.Profiler;
import com.aoindustries.sql.DatabaseAccess;
import com.aoindustries.util.PropertiesUtils;
import com.aoindustries.util.StringUtility;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
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
        synchronized (MasterConfiguration.class) {
            if (props == null) props = PropertiesUtils.loadFromResource(MasterConfiguration.class, "aoserv-master.properties");
	        return props.getProperty(name);
        }
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
    public static InetAddress getLocalIp() throws IOException {
        try {
            return InetAddress.valueOf(getProperty("aoserv.master.local_ip"));
        } catch(ValidationException e) {
            IOException exc = new IOException(e.getLocalizedMessage());
            exc.initCause(e);
            throw exc;
        }
    }

    public static List<String> getBinds(String protocol) throws IOException {
        return StringUtility.splitStringCommaSpace(getProperty("aoserv.master."+protocol+".bind"));
    }

    public static List<Integer> getPorts(String protocol) throws IOException {
        String ports = getProperty("aoserv.master."+protocol+".ports");
        List<String> strings = StringUtility.splitStringCommaSpace(ports);
        List<Integer> ints = new ArrayList<Integer>(strings.size());
        for(int c=0,len=strings.size();c<len;c++) {
            ints.add(Integer.parseInt(strings.get(c)));
        }
        return ints;
    }

    public static int getHistorySize() throws IOException {
        return Integer.parseInt(getProperty("aoserv.master.history.size"));
    }

    public static AccountingCode getRootBusiness() throws IOException {
        try {
            return AccountingCode.valueOf(getProperty("aoserv.master.businesses.root"));
        } catch(ValidationException e) {
            IOException exc = new IOException(e.getLocalizedMessage());
            exc.initCause(e);
            throw exc;
        }
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

    public static String getDaemonKey(DatabaseAccess database, int aoServer) throws IOException, SQLException {
        return getProperty("aoserv.daemon.client.key."+ServerHandler.getHostnameForAOServer(database, aoServer));
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
