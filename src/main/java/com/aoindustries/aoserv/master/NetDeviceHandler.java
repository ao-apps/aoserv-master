/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2008-2013, 2015, 2017, 2018, 2019, 2020, 2021, 2022  AO Industries, Inc.
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
 * along with aoserv-master.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.master;

import com.aoapps.dbc.DatabaseConnection;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import java.io.IOException;
import java.sql.SQLException;

/**
 * The <code>NetDeviceHandler</code> handles all the accesses to the <code>net.Device</code> table.
 *
 * @author  AO Industries, Inc.
 */
public final class NetDeviceHandler {

	/** Make no instances. */
	private NetDeviceHandler() {throw new AssertionError();}

	public static String getDeviceBondingReport(DatabaseConnection conn, RequestSource source, int device) throws IOException, SQLException {
		int host = getHostForDevice(conn, device);
		if(!NetHostHandler.isLinuxServer(conn, host)) throw new SQLException("Host is not a Linux server: " + host);
		NetHostHandler.checkAccessHost(conn, source, "getDeviceBondingReport", host);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, host);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.getNetDeviceBondingReport(device);
	}

	public static String getDeviceStatisticsReport(DatabaseConnection conn, RequestSource source, int device) throws IOException, SQLException {
		int host = getHostForDevice(conn, device);
		if(!NetHostHandler.isLinuxServer(conn, host)) throw new SQLException("Host is not a Linux server: " + host);
		NetHostHandler.checkAccessHost(conn, source, "getDeviceStatisticsReport", host);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, host);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.getNetDeviceStatisticsReport(device);
	}

	public static int getHostForDevice(DatabaseConnection conn, int device) throws IOException, SQLException {
		return conn.queryInt("select server from net.\"Device\" where id=?", device);
	}
}
