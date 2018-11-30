/*
 * Copyright 2012, 2013, 2014, 2015, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.linux.Server;
import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.daemon.client.AOServDaemonProtocol;
import com.aoindustries.dbc.DatabaseConnection;
import java.io.IOException;
import java.sql.SQLException;

/**
 * The <code>ServerHandler</code> handles all the accesses to the Host tables.
 *
 * @author  AO Industries, Inc.
 */
final public class VirtualServerHandler {

	// private static final Logger logger = LogFactory.getLogger(VirtualServerHandler.class);

	private VirtualServerHandler() {
	}

	/*
	public static void checkAccessVirtualServer(DatabaseConnection conn, RequestSource source, String action, int virtualServer) throws IOException, SQLException {
		if(!canAccessVirtualServer(conn, source, virtualServer)) {
			String message=
				"business_administrator.username="
				+source.getUsername()
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

	public static int getVirtualServerForVirtualDisk(DatabaseConnection conn, int virtualDiskId) throws IOException, SQLException {
		return conn.executeIntQuery("select virtual_server from infrastructure.\"VirtualDisk\" where id=?", virtualDiskId);
	}

	public static String getDeviceForVirtualDisk(DatabaseConnection conn, int virtualDiskId) throws IOException, SQLException {
		return conn.executeStringQuery("select device from infrastructure.\"VirtualDisk\" where id=?", virtualDiskId);
	}

	public static Server.DaemonAccess requestVncConsoleDaemonAccess(
		DatabaseConnection conn,
		RequestSource source,
		int virtualServer
	) throws IOException, SQLException {
		// The user must have proper permissions
		BusinessHandler.checkPermission(conn, source, "requestVncConsoleDaemonAccess", Permission.Name.vnc_console);
		// The business must have proper access
		boolean canVncConsole=BusinessHandler.canBusinessServer(conn, source, virtualServer, "can_vnc_console");
		if(!canVncConsole) throw new SQLException("Not allowed to VNC console to "+virtualServer);
		// TODO: Must not be a disabled server
		// Must be a virtual server with VNC enabled
		String vncPassword = conn.executeStringQuery("select vnc_password from infrastructure.\"VirtualServer\" where server=?", virtualServer);
		if(vncPassword==null) throw new SQLException("Virtual server VNC is disabled: "+virtualServer);
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		return DaemonHandler.grantDaemonAccess(
			conn,
			primaryPhysicalServer,
			null,
			AOServDaemonProtocol.VNC_CONSOLE,
			ServerHandler.getNameForServer(conn, virtualServer),
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
		BusinessHandler.checkPermission(conn, source, "createVirtualServer", Permission.Name.control_virtual_server);
		// The business must have proper access
		boolean canControlVirtualServer=BusinessHandler.canBusinessServer(conn, source, virtualServer, "can_control_virtual_server");
		if(!canControlVirtualServer) throw new SQLException("Not allowed to control "+virtualServer);
		// TODO: Must not be a disabled server
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		return DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer).createVirtualServer(ServerHandler.getNameForServer(conn, virtualServer));
	}

	public static String rebootVirtualServer(
		DatabaseConnection conn,
		RequestSource source,
		int virtualServer
	) throws IOException, SQLException {
		// The user must have proper permissions
		BusinessHandler.checkPermission(conn, source, "rebootVirtualServer", Permission.Name.control_virtual_server);
		// The business must have proper access
		boolean canControlVirtualServer=BusinessHandler.canBusinessServer(conn, source, virtualServer, "can_control_virtual_server");
		if(!canControlVirtualServer) throw new SQLException("Not allowed to control "+virtualServer);
		// TODO: Must not be a disabled server
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		return DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer).rebootVirtualServer(ServerHandler.getNameForServer(conn, virtualServer));
	}

	public static String shutdownVirtualServer(
		DatabaseConnection conn,
		RequestSource source,
		int virtualServer
	) throws IOException, SQLException {
		// The user must have proper permissions
		BusinessHandler.checkPermission(conn, source, "shutdownVirtualServer", Permission.Name.control_virtual_server);
		// The business must have proper access
		boolean canControlVirtualServer=BusinessHandler.canBusinessServer(conn, source, virtualServer, "can_control_virtual_server");
		if(!canControlVirtualServer) throw new SQLException("Not allowed to control "+virtualServer);
		// TODO: Must not be a disabled server
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		return DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer).shutdownVirtualServer(ServerHandler.getNameForServer(conn, virtualServer));
	}

	public static String destroyVirtualServer(
		DatabaseConnection conn,
		RequestSource source,
		int virtualServer
	) throws IOException, SQLException {
		// The user must have proper permissions
		BusinessHandler.checkPermission(conn, source, "destroyVirtualServer", Permission.Name.control_virtual_server);
		// The business must have proper access
		boolean canControlVirtualServer=BusinessHandler.canBusinessServer(conn, source, virtualServer, "can_control_virtual_server");
		if(!canControlVirtualServer) throw new SQLException("Not allowed to control "+virtualServer);
		// TODO: Must not be a disabled server
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		return DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer).destroyVirtualServer(ServerHandler.getNameForServer(conn, virtualServer));
	}

	public static String pauseVirtualServer(
		DatabaseConnection conn,
		RequestSource source,
		int virtualServer
	) throws IOException, SQLException {
		// The user must have proper permissions
		BusinessHandler.checkPermission(conn, source, "pauseVirtualServer", Permission.Name.control_virtual_server);
		// The business must have proper access
		boolean canControlVirtualServer=BusinessHandler.canBusinessServer(conn, source, virtualServer, "can_control_virtual_server");
		if(!canControlVirtualServer) throw new SQLException("Not allowed to control "+virtualServer);
		// TODO: Must not be a disabled server
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		return DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer).pauseVirtualServer(ServerHandler.getNameForServer(conn, virtualServer));
	}

	public static String unpauseVirtualServer(
		DatabaseConnection conn,
		RequestSource source,
		int virtualServer
	) throws IOException, SQLException {
		// The user must have proper permissions
		BusinessHandler.checkPermission(conn, source, "unpauseVirtualServer", Permission.Name.control_virtual_server);
		// The business must have proper access
		boolean canControlVirtualServer=BusinessHandler.canBusinessServer(conn, source, virtualServer, "can_control_virtual_server");
		if(!canControlVirtualServer) throw new SQLException("Not allowed to control "+virtualServer);
		// TODO: Must not be a disabled server
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		return DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer).unpauseVirtualServer(ServerHandler.getNameForServer(conn, virtualServer));
	}

	public static int getVirtualServerStatus(
		DatabaseConnection conn,
		RequestSource source,
		int virtualServer
	) throws IOException, SQLException {
		// The user must have proper permissions
		BusinessHandler.checkPermission(conn, source, "getVirtualServerStatus", Permission.Name.get_virtual_server_status);
		// TODO: Must not be a disabled server
		// Find current location of server
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		// Grant access to the Xen outer server
		return DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer).getVirtualServerStatus(ServerHandler.getNameForServer(conn, virtualServer));
	}

	public static long verifyVirtualDisk(
		DatabaseConnection conn,
		RequestSource source,
		int virtualDisk
	) throws IOException, SQLException {
		int virtualServer = getVirtualServerForVirtualDisk(conn, virtualDisk);
		// The user must have proper permissions
		BusinessHandler.checkPermission(conn, source, "verifyVirtualDisk", Permission.Name.control_virtual_server);
		// The business must have proper access
		boolean canControlVirtualServer=BusinessHandler.canBusinessServer(conn, source, virtualServer, "can_control_virtual_server");
		if(!canControlVirtualServer) throw new SQLException("Not allowed to control "+virtualServer);
		// Must be a cluster admin
		ClusterHandler.checkClusterAdmin(conn, source, "verifyVirtualDisk");
		// TODO: Must not be a disabled server
		// Lookup values
		String virtualServerName = ServerHandler.getNameForServer(conn, virtualServer);
		String device = getDeviceForVirtualDisk(conn, virtualDisk);
		// Find current location of primary and secondary servers
		int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
		int secondaryPhysicalServer = ClusterHandler.getSecondaryPhysicalServer(virtualServer);
		// Begin verification, getting Unix time in seconds
		long lastVerified = DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer).verifyVirtualDisk(
			virtualServerName,
			device
		);
		// Update the verification time on the secondary
		DaemonHandler.getDaemonConnector(conn, secondaryPhysicalServer).updateVirtualDiskLastVerified(
			virtualServerName,
			device,
			lastVerified
		);
		// Return as Java timestamp
		return lastVerified * 1000;
	}
}