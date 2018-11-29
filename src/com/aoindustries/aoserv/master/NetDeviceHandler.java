/*
 * Copyright 2008-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

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
		if(!ServerHandler.isAOServer(conn, server)) throw new SQLException("Server is not an AOServer: "+server);
		ServerHandler.checkAccessServer(conn, source, "getNetDeviceBondingReport", server);

		return DaemonHandler.getDaemonConnector(conn, server).getNetDeviceBondingReport(device);
	}

	public static String getNetDeviceStatisticsReport(DatabaseConnection conn, RequestSource source, int device) throws IOException, SQLException {
		int server = getServerForNetDevice(conn, device);
		if(!ServerHandler.isAOServer(conn, server)) throw new SQLException("Server is not an AOServer: "+server);
		ServerHandler.checkAccessServer(conn, source, "getNetDeviceStatisticsReport", server);

		return DaemonHandler.getDaemonConnector(conn, server).getNetDeviceStatisticsReport(device);
	}

	public static int getServerForNetDevice(DatabaseConnection conn, int device) throws IOException, SQLException {
		return conn.executeIntQuery("select server from net.\"Device\" where id=?", device);
	}
}
