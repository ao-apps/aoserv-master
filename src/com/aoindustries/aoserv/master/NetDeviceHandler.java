/*
 * Copyright 2008-2013, 2015, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import com.aoindustries.dbc.DatabaseConnection;
import java.io.IOException;
import java.sql.SQLException;

/**
 * The <code>NetDeviceHandler</code> handles all the accesses to the <code>net.Device</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class NetDeviceHandler {

	private NetDeviceHandler() {
	}

	public static String getNetDeviceBondingReport(DatabaseConnection conn, RequestSource source, int device) throws IOException, SQLException {
		int server = getServerForNetDevice(conn, device);
		if(!ServerHandler.isAOServer(conn, server)) throw new SQLException("Host is not an Server: "+server);
		ServerHandler.checkAccessServer(conn, source, "getNetDeviceBondingReport", server);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, server);
		conn.releaseConnection();
		return daemonConnector.getNetDeviceBondingReport(device);
	}

	public static String getNetDeviceStatisticsReport(DatabaseConnection conn, RequestSource source, int device) throws IOException, SQLException {
		int server = getServerForNetDevice(conn, device);
		if(!ServerHandler.isAOServer(conn, server)) throw new SQLException("Host is not an Server: "+server);
		ServerHandler.checkAccessServer(conn, source, "getNetDeviceStatisticsReport", server);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, server);
		conn.releaseConnection();
		return daemonConnector.getNetDeviceStatisticsReport(device);
	}

	public static int getServerForNetDevice(DatabaseConnection conn, int device) throws IOException, SQLException {
		return conn.executeIntQuery("select server from net.\"Device\" where id=?", device);
	}
}
