/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2017, 2018, 2019, 2020  AO Industries, Inc.
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

import com.aoindustries.aoserv.client.linux.Server;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import com.aoindustries.dbc.DatabaseAccess;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.AOPool;
import com.aoindustries.net.HostAddress;
import com.aoindustries.net.InetAddress;
import com.aoindustries.net.Port;
import java.io.IOException;
import java.security.SecureRandom;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * The <code>AOServDaemonHandler</code> handles all the accesses to the daemons.
 *
 * @author  AO Industries, Inc.
 */
final public class DaemonHandler {

	private DaemonHandler() {
	}

	/**
	 * The amount of time before a daemon will be accessed again once
	 * flagged as unavailable.
	 */
	public static final int DAEMON_RETRY_DELAY=5*1000; // Used to be 60*1000

	private static final Map<Integer,AOServDaemonConnector> connectors = new HashMap<>();

	public static int getDaemonConcurrency() {
		int total=0;
		Iterator<Integer> I=connectors.keySet().iterator();
		while(I.hasNext()) {
			Integer key=I.next();
			total+=connectors.get(key).getConcurrency();
		}
		return total;
	}

	public static int getDaemonConnections() {
		int total=0;
		Iterator<Integer> I=connectors.keySet().iterator();
		while(I.hasNext()) {
			Integer key=I.next();
			total+=connectors.get(key).getConnectionCount();
		}
		return total;
	}

	public static HostAddress getDaemonConnectAddress(DatabaseAccess database, int linuxServer) throws IOException, SQLException {
		HostAddress address = database.executeObjectQuery(
			ObjectFactories.hostAddressFactory,
			"select daemon_connect_address from linux.\"Server\" where server=?",
			linuxServer
		);
		if(address!=null) return address;
		InetAddress ip = database.executeObjectQuery(ObjectFactories.inetAddressFactory,
			"select\n"
			+ "  host(ia.\"inetAddress\")\n"
			+ "from\n"
			+ "  linux.\"Server\" ao,\n"
			+ "  net.\"Bind\" nb,\n"
			+ "  net.\"IpAddress\" ia\n"
			+ "where\n"
			+ "  ao.server=?\n"
			+ "  and ao.daemon_connect_bind=nb.id\n"
			+ "  and nb.\"ipAddress\"=ia.id",
			linuxServer
		);
		if(ip==null) throw new SQLException("Unable to find daemon IP address for Server: "+linuxServer);
		if(ip.isUnspecified()) {
			ip = database.executeObjectQuery(ObjectFactories.inetAddressFactory,
				"select\n"
				+ "  host(ia.\"inetAddress\")\n"
				+ "from\n"
				+ "  linux.\"Server\" ao,\n"
				+ "  net.\"Bind\" nb,\n"
				+ "  linux.\"Server\" ao2,\n"
				+ "  net.\"Device\" nd,\n"
				+ "  net.\"IpAddress\" ia\n"
				+ "where\n"
				+ "  ao.server=?\n"
				+ "  and ao.daemon_connect_bind=nb.id\n"
				+ "  and nb.server=ao2.server\n"
				+ "  and ao2.server=nd.server\n"
				+ "  and ao2.\"daemonDeviceId\"=nd.\"deviceId\"\n"
				+ "  and nd.id=ia.device\n"
				+ "  and not ia.\"isAlias\"\n"
				+ "limit 1",
				linuxServer
			);
			if(ip==null) throw new SQLException("Unable to find daemon IP address for Server: "+linuxServer);
		}
		return HostAddress.valueOf(ip);
	}

	public static Port getDaemonConnectorPort(DatabaseAccess database, int linuxServer) throws IOException, SQLException {
		return database.executeObjectQuery(
			ObjectFactories.portFactory,
			"select\n"
			+ "  nb.port,\n"
			+ "  nb.net_protocol\n"
			+ "from\n"
			+ "  linux.\"Server\" ao,\n"
			+ "  net.\"Bind\" nb\n"
			+ "where\n"
			+ "  ao.server=?\n"
			+ "  and ao.daemon_connect_bind=nb.id",
			linuxServer
		);
	}

	public static String getDaemonConnectorProtocol(DatabaseAccess database, int linuxServer) throws IOException, SQLException {
		return database.executeStringQuery(
			"select\n"
			+ "  nb.app_protocol\n"
			+ "from\n"
			+ "  linux.\"Server\" ao,\n"
			+ "  net.\"Bind\" nb\n"
			+ "where\n"
			+ "  ao.server=?\n"
			+ "  and ao.daemon_connect_bind=nb.id",
			linuxServer
		);
	}

	public static int getDaemonConnectorPoolSize(DatabaseAccess database, int linuxServer) throws IOException, SQLException {
		return database.executeIntQuery(
			"select\n"
			+ "  pool_size\n"
			+ "from\n"
			+ "  linux.\"Server\"\n"
			+ "where\n"
			+ "  server=?",
			linuxServer
		);
	}

	public static AOServDaemonConnector getDaemonConnector(DatabaseAccess database, int linuxServer) throws IOException, SQLException {
		Integer I = linuxServer;
		synchronized(DaemonHandler.class) {
			AOServDaemonConnector O=connectors.get(I);
			if(O!=null) return O;
			AOServDaemonConnector conn = AOServDaemonConnector.getConnector(
				getDaemonConnectAddress(database, linuxServer),
				MasterConfiguration.getLocalIp(),
				getDaemonConnectorPort(database, linuxServer),
				getDaemonConnectorProtocol(database, linuxServer),
				MasterConfiguration.getDaemonKey(database, linuxServer),
				getDaemonConnectorPoolSize(database, linuxServer),
				AOPool.DEFAULT_MAX_CONNECTION_AGE,
				MasterConfiguration.getSSLTruststorePath(),
				MasterConfiguration.getSSLTruststorePassword()
			);
			connectors.put(I, conn);
			return conn;
		}
	}

	public static int getDaemonConnects() {
		int total=0;
		Iterator<Integer> I=connectors.keySet().iterator();
		while(I.hasNext()) {
			Integer key=I.next();
			total+=connectors.get(key).getConnects();
		}
		return total;
	}

	public static int getDaemonCount() {
		return connectors.size();
	}

	public static int getDaemonMaxConcurrency() {
		int total=0;
		Iterator<Integer> I=connectors.keySet().iterator();
		while(I.hasNext()) {
			Integer key=I.next();
			total+=connectors.get(key).getMaxConcurrency();
		}
		return total;
	}

	public static int getDaemonPoolSize() {
		int total=0;
		Iterator<Integer> I=connectors.keySet().iterator();
		while(I.hasNext()) {
			Integer key=I.next();
			total+=connectors.get(key).getPoolSize();
		}
		return total;
	}

	public static long getDaemonTotalTime() {
		long total=0;
		Iterator<Integer> I=connectors.keySet().iterator();
		while(I.hasNext()) {
			Integer key=I.next();
			total+=connectors.get(key).getTotalTime();
		}
		return total;
	}

	public static long getDaemonTransactions() {
		long total=0;
		Iterator<Integer> I=connectors.keySet().iterator();
		while(I.hasNext()) {
			Integer key=I.next();
			total+=connectors.get(key).getTransactionCount();
		}
		return total;
	}

	private static final Map<Integer,Long> downDaemons = new HashMap<>();

	public static void invalidateTable(Table.TableID tableID) {
		if(
			tableID==Table.TableID.AO_SERVERS
			|| tableID==Table.TableID.IP_ADDRESSES
			|| tableID==Table.TableID.NET_BINDS
		) {
			synchronized(DaemonHandler.class) {
				connectors.clear();
			}
		}
	}

	/**
	 * The availability of daemons is maintained to avoid repeatedly trying to access
	 * a daemon that is not responding while other daemons could be used.
	 */
	public static boolean isDaemonAvailable(int linuxServer) {
		Integer I = linuxServer;
		synchronized(downDaemons) {
			Long O=downDaemons.get(I);
			if(O!=null) {
				long downTime=System.currentTimeMillis() - O;
				if(downTime<0) {
					downDaemons.remove(I);
					return true;
				}
				if(downTime<DAEMON_RETRY_DELAY) return false;
				downDaemons.remove(I);
			}
		}
		return true;
	}

	public static void flagDaemonAsDown(int linuxServer) throws IOException {
		Integer I = linuxServer;
		synchronized(downDaemons) {
			downDaemons.put(I, System.currentTimeMillis());
		}
	}

	public static int getDownDaemonCount() {
		synchronized(downDaemons) {
			return downDaemons.size();
		}
	}

	private final static Map<Long,Long> recentKeys = new HashMap<>();
	private static long lastKeyCleanTime = -1;

	/**
	 * @param connectAddress Overridden connect address or <code>null</code> to use the default
	 */
	public static Server.DaemonAccess grantDaemonAccess(
		DatabaseConnection conn,
		int linuxServer,
		HostAddress connectAddress,
		int daemonCommandCode,
		String param1,
		String param2,
		String param3,
		String param4
	) throws IOException, SQLException {
		long key;
		synchronized(recentKeys) {
			long currentTime=System.currentTimeMillis();
			if(lastKeyCleanTime==-1) lastKeyCleanTime=currentTime;
			else {
				long timeSince=currentTime-lastKeyCleanTime;
				if(timeSince<0 || timeSince>=(5L*60*1000)) {
					// Clean up the entries over one hour old
					Iterator<Long> I=recentKeys.keySet().iterator();
					while(I.hasNext()) {
						Long keyObj=I.next();
						long time = recentKeys.get(keyObj);
						timeSince=currentTime-time;
						if(timeSince<0 || timeSince>=(60L*60*1000)) {
							I.remove();
						}
					}
					lastKeyCleanTime=currentTime;
				}
			}

			// Generate the key
			SecureRandom secureRandom = MasterServer.getSecureRandom();
			while(true) {
				key=secureRandom.nextLong();
				Long L = key;
				if(!recentKeys.containsKey(L)) {
					recentKeys.put(L, System.currentTimeMillis());
					break;
				}
			}
		}

		// Send the key to the daemon
		AOServDaemonConnector daemonConnector = getDaemonConnector(conn, linuxServer);
		conn.releaseConnection();
		daemonConnector.grantDaemonAccess(key, daemonCommandCode, param1, param2, param3, param4);

		return new Server.DaemonAccess(
			getDaemonConnectorProtocol(conn, linuxServer),
			connectAddress!=null ? connectAddress : getDaemonConnectAddress(conn, linuxServer),
			getDaemonConnectorPort(conn, linuxServer),
			key
		);
	}
}
