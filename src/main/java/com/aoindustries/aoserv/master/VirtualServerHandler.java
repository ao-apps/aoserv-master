/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2012, 2013, 2014, 2015, 2018, 2019, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.dbc.DatabaseConnection;
import com.aoindustries.aoserv.client.linux.Server;
import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import com.aoindustries.aoserv.daemon.client.AOServDaemonProtocol;
import java.io.IOException;
import java.sql.SQLException;

/**
 * The <code>ServerHandler</code> handles all the accesses to the Host tables.
 *
 * @author  AO Industries, Inc.
 */
final public class VirtualServerHandler {

	// private static final Logger logger = Logger.getLogger(VirtualServerHandler.class.getName());

	private VirtualServerHandler() {
	}

	/*
	public static void checkAccessVirtualServer(DatabaseConnection conn, RequestSource source, String action, int virtualServer) throws IOException, SQLException {
		if(!canAccessVirtualServer(conn, source, virtualServer)) {
			String message=
				"currentAdministrator="
				+source.getCurrentAdministrator()
				+" is not allowed to access virtual server: action='"
				+action
				+", virtual_server.server="
				+virtualServer
			;
			throw new SQLException(message);
		}
	}

	public static boolean canAccessVirtualServer(DatabaseConnection conn, RequestSource source, int virtualServer) throws IOException, SQLException {
		return ServerHandler.canAccessServer(conn, source, virtualServer);
	}
	 */

	public static int getVirtualServerForVirtualDisk(DatabaseConnection conn, int virtualDisk) throws IOException, SQLException {
		return conn.queryInt("select virtual_server from infrastructure.\"VirtualDisk\" where id=?", virtualDisk);
	}

	public static String getDeviceForVirtualDisk(DatabaseConnection conn, int virtualDisk) throws IOException, SQLException {
		return conn.queryString("select device from infrastructure.\"VirtualDisk\" where id=?", virtualDisk);
	}

	public static Server.DaemonAccess requestVncConsoleDaemonAccess(
		DatabaseConnection conn,
		RequestSource source,
		int virtualServer
	) throws IOException, SQLException {
		// The user must have proper permissions
		AccountHandler.checkPermission(conn, source, "requestVncConsoleDaemonAccess", Permission.Name.vnc_console);
		// The business must have proper access
		boolean canVncConsole=AccountHandler.canAccountHost_column(conn, source, virtualServer, "can_vnc_console");
		if(!canVncConsole) throw new SQLException("Not allowed to VNC console to "+virtualServer);
		// TODO: Must not be a disabled server
		// Must be a virtual server with VNC enabled
		String vncPassword = conn.queryString("select vnc_password from infrastructure.\"VirtualServer\" where server=?", virtualServer);
		if(vncPassword==null) throw new SQLException("Virtual server VNC is disabled: "+virtualServer);
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		return DaemonHandler.grantDaemonAccess(
			conn,
			primaryPhysicalServer,
			null,
			AOServDaemonProtocol.VNC_CONSOLE,
			NetHostHandler.getNameForHost(conn, virtualServer),
			null,
			null,
			null
		);
	}

	public static String createVirtualServer(
		DatabaseConnection conn,
		RequestSource source,
		int virtualServer
	) throws IOException, SQLException {
		// The user must have proper permissions
		AccountHandler.checkPermission(conn, source, "createVirtualServer", Permission.Name.control_virtual_server);
		// The business must have proper access
		boolean canControlVirtualServer=AccountHandler.canAccountHost_column(conn, source, virtualServer, "can_control_virtual_server");
		if(!canControlVirtualServer) throw new SQLException("Not allowed to control "+virtualServer);
		// TODO: Must not be a disabled server
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		String virtualServerName = NetHostHandler.getNameForHost(conn, virtualServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.createVirtualServer(virtualServerName);
	}

	public static String rebootVirtualServer(
		DatabaseConnection conn,
		RequestSource source,
		int virtualServer
	) throws IOException, SQLException {
		// The user must have proper permissions
		AccountHandler.checkPermission(conn, source, "rebootVirtualServer", Permission.Name.control_virtual_server);
		// The business must have proper access
		boolean canControlVirtualServer=AccountHandler.canAccountHost_column(conn, source, virtualServer, "can_control_virtual_server");
		if(!canControlVirtualServer) throw new SQLException("Not allowed to control "+virtualServer);
		// TODO: Must not be a disabled server
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		String virtualServerName = NetHostHandler.getNameForHost(conn, virtualServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.rebootVirtualServer(virtualServerName);
	}

	public static String shutdownVirtualServer(
		DatabaseConnection conn,
		RequestSource source,
		int virtualServer
	) throws IOException, SQLException {
		// The user must have proper permissions
		AccountHandler.checkPermission(conn, source, "shutdownVirtualServer", Permission.Name.control_virtual_server);
		// The business must have proper access
		boolean canControlVirtualServer=AccountHandler.canAccountHost_column(conn, source, virtualServer, "can_control_virtual_server");
		if(!canControlVirtualServer) throw new SQLException("Not allowed to control "+virtualServer);
		// TODO: Must not be a disabled server
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		String virtualServerName = NetHostHandler.getNameForHost(conn, virtualServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.shutdownVirtualServer(virtualServerName);
	}

	public static String destroyVirtualServer(
		DatabaseConnection conn,
		RequestSource source,
		int virtualServer
	) throws IOException, SQLException {
		// The user must have proper permissions
		AccountHandler.checkPermission(conn, source, "destroyVirtualServer", Permission.Name.control_virtual_server);
		// The business must have proper access
		boolean canControlVirtualServer=AccountHandler.canAccountHost_column(conn, source, virtualServer, "can_control_virtual_server");
		if(!canControlVirtualServer) throw new SQLException("Not allowed to control "+virtualServer);
		// TODO: Must not be a disabled server
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		String virtualServerName = NetHostHandler.getNameForHost(conn, virtualServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.destroyVirtualServer(virtualServerName);
	}

	public static String pauseVirtualServer(
		DatabaseConnection conn,
		RequestSource source,
		int virtualServer
	) throws IOException, SQLException {
		// The user must have proper permissions
		AccountHandler.checkPermission(conn, source, "pauseVirtualServer", Permission.Name.control_virtual_server);
		// The business must have proper access
		boolean canControlVirtualServer=AccountHandler.canAccountHost_column(conn, source, virtualServer, "can_control_virtual_server");
		if(!canControlVirtualServer) throw new SQLException("Not allowed to control "+virtualServer);
		// TODO: Must not be a disabled server
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		String virtualServerName = NetHostHandler.getNameForHost(conn, virtualServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.pauseVirtualServer(virtualServerName);
	}

	public static String unpauseVirtualServer(
		DatabaseConnection conn,
		RequestSource source,
		int virtualServer
	) throws IOException, SQLException {
		// The user must have proper permissions
		AccountHandler.checkPermission(conn, source, "unpauseVirtualServer", Permission.Name.control_virtual_server);
		// The business must have proper access
		boolean canControlVirtualServer=AccountHandler.canAccountHost_column(conn, source, virtualServer, "can_control_virtual_server");
		if(!canControlVirtualServer) throw new SQLException("Not allowed to control "+virtualServer);
		// TODO: Must not be a disabled server
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		String virtualServerName = NetHostHandler.getNameForHost(conn, virtualServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.unpauseVirtualServer(virtualServerName);
	}

	public static int getVirtualServerStatus(
		DatabaseConnection conn,
		RequestSource source,
		int virtualServer
	) throws IOException, SQLException {
		// The user must have proper permissions
		AccountHandler.checkPermission(conn, source, "getVirtualServerStatus", Permission.Name.get_virtual_server_status);
		// TODO: Must not be a disabled server
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		String virtualServerName = NetHostHandler.getNameForHost(conn, virtualServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.getVirtualServerStatus(virtualServerName);
	}

	public static long verifyVirtualDisk(
		DatabaseConnection conn,
		RequestSource source,
		int virtualDisk
	) throws IOException, SQLException {
		int virtualServer = getVirtualServerForVirtualDisk(conn, virtualDisk);
		// The user must have proper permissions
		AccountHandler.checkPermission(conn, source, "verifyVirtualDisk", Permission.Name.control_virtual_server);
		// The business must have proper access
		boolean canControlVirtualServer=AccountHandler.canAccountHost_column(conn, source, virtualServer, "can_control_virtual_server");
		if(!canControlVirtualServer) throw new SQLException("Not allowed to control "+virtualServer);
		// Must be a cluster admin
		ClusterHandler.checkClusterAdmin(conn, source, "verifyVirtualDisk");
		// TODO: Must not be a disabled server
		// Lookup values
		String virtualServerName = NetHostHandler.getNameForHost(conn, virtualServer);
		String device = getDeviceForVirtualDisk(conn, virtualDisk);
		// Find current location of primary and secondary servers
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		int secondaryPhysicalServer = ClusterHandler.getSecondaryPhysicalServer(virtualServer);
		// Begin verification, getting Unix time in seconds
		AOServDaemonConnector primaryDaemonConnector = DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer);
		AOServDaemonConnector secondaryDaemonConnector = DaemonHandler.getDaemonConnector(conn, secondaryPhysicalServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		long lastVerified = primaryDaemonConnector.verifyVirtualDisk(virtualServerName, device);
		// Update the verification time on the secondary
		secondaryDaemonConnector.updateVirtualDiskLastVerified(virtualServerName, device, lastVerified);
		// Return as Java timestamp
		return lastVerified * 1000;
	}
}