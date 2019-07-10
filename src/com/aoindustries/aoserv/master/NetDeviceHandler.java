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

	public static String getDeviceBondingReport(DatabaseConnection conn, RequestSource source, int device) throws IOException, SQLException {
		int host = getHostForDevice(conn, device);
		if(!NetHostHandler.isLinuxServer(conn, host)) throw new SQLException("Host is not a Linux server: " + host);
		NetHostHandler.checkAccessHost(conn, source, "getDeviceBondingReport", host);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, host);
		conn.releaseConnection();
		return daemonConnector.getNetDeviceBondingReport(device);
	}

	public static String getDeviceStatisticsReport(DatabaseConnection conn, RequestSource source, int device) throws IOException, SQLException {
		int host = getHostForDevice(conn, device);
		if(!NetHostHandler.isLinuxServer(conn, host)) throw new SQLException("Host is not a Linux server: " + host);
		NetHostHandler.checkAccessHost(conn, source, "getDeviceStatisticsReport", host);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, host);
		conn.releaseConnection();
		return daemonConnector.getNetDeviceStatisticsReport(device);
	}

	public static int getHostForDevice(DatabaseConnection conn, int device) throws IOException, SQLException {
		return conn.executeIntQuery("select server from net.\"Device\" where id=?", device);
	}
}
