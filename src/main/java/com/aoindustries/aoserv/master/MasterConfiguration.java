/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2017, 2018, 2019, 2020, 2021  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of aoserv-master.
 *
 * aoserv-master is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * aoserv-master is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with aoserv-master.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.master;

import com.aoapps.dbc.DatabaseAccess;
import com.aoapps.hodgepodge.io.AOPool;
import com.aoapps.lang.Strings;
import com.aoapps.lang.util.PropertiesUtils;
import com.aoapps.lang.validation.ValidationException;
import com.aoapps.net.DomainName;
import com.aoapps.net.InetAddress;
import com.aoapps.security.UnprotectedKey;
import com.aoindustries.aoserv.client.account.Account;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The configuration for all AOServ processes is stored in a properties file.
 *
 * @author  AO Industries, Inc.
 */
public final class MasterConfiguration {

	private static final String PROPERTIES_FILENAME = "aoserv-master.properties";

	private static Properties props;

	private MasterConfiguration() {
	}

	private static String getProperty(String name) throws IOException {
		synchronized (MasterConfiguration.class) {
			if (props == null) props = PropertiesUtils.loadFromResource(MasterConfiguration.class, PROPERTIES_FILENAME);
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
		return Strings.splitCommaSpace(getProperty("aoserv.master.protocols"));
	}

	/**
	 * Gets the local IP address used for outgoing connections to the daemons.
	 */
	public static InetAddress getLocalIp() throws IOException {
		try {
			return InetAddress.valueOf(getProperty("aoserv.master.local_ip"));
		} catch(ValidationException e) {
			throw new IOException(e.getLocalizedMessage(), e);
		}
	}

	public static List<String> getBinds(String protocol) throws IOException {
		return Strings.splitCommaSpace(getProperty("aoserv.master."+protocol+".bind"));
	}

	public static List<Integer> getPorts(String protocol) throws IOException {
		String ports = getProperty("aoserv.master."+protocol+".ports");
		List<String> strings = Strings.splitCommaSpace(ports);
		List<Integer> ints = new ArrayList<>(strings.size());
		for(int c=0,len=strings.size();c<len;c++) {
			ints.add(Integer.parseInt(strings.get(c)));
		}
		return ints;
	}

	public static Account.Name getRootAccount() throws IOException {
		try {
			// TODO: Rename this property
			return Account.Name.valueOf(getProperty("aoserv.master.businesses.root"));
		} catch(ValidationException e) {
			throw new IOException(e.getLocalizedMessage(), e);
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

	private static final ConcurrentMap<Integer, UnprotectedKey> daemonKeys = new ConcurrentHashMap<>();
	public static UnprotectedKey getDaemonKey(DatabaseAccess database, int linuxServer) throws IOException, SQLException {
		UnprotectedKey daemonKey = daemonKeys.get(linuxServer);
		if(daemonKey == null) {
			DomainName hostname = NetHostHandler.getHostnameForLinuxServer(database, linuxServer);
			String propValue = getProperty("aoserv.daemon.client.key." + hostname);
			if(propValue == null || (propValue = propValue.trim()).isEmpty()) throw new IOException("No daemon key in " + PROPERTIES_FILENAME + " for " + hostname);
			daemonKey = new UnprotectedKey(Base64.getDecoder().decode(propValue));
			UnprotectedKey existing = daemonKeys.putIfAbsent(linuxServer, daemonKey);
			if(existing != null) daemonKey = existing;
		}
		return daemonKey;
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
