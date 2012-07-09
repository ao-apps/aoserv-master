/*
 * Copyright 2012 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.AOServPermission;
import com.aoindustries.aoserv.client.AOServer;
import com.aoindustries.aoserv.daemon.client.AOServDaemonProtocol;
import com.aoindustries.sql.DatabaseConnection;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * The <code>ServerHandler</code> handles all the accesses to the Server tables.
 *
 * @author  AO Industries, Inc.
 */
final public class VirtualServerHandler {

    private static final Logger logger = LogFactory.getLogger(VirtualServerHandler.class);

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

    public static AOServer.DaemonAccess requestVncConsoleDaemonAccess(
        DatabaseConnection conn,
        RequestSource source,
        int virtualServer
    ) throws IOException, SQLException {
        // The user must have proper permissions
        BusinessHandler.checkPermission(conn, source, "requestVncConsoleDaemonAccess", AOServPermission.Permission.vnc_console);
        // The business must have proper access
        boolean canVncConsole=BusinessHandler.canBusinessServer(conn, source, virtualServer, "can_vnc_console");
        if(!canVncConsole) throw new SQLException("Not allowed to VNC console to "+virtualServer);
        // TODO: Must not be a disabled server
        // Must be a virtual server with VNC enabled
        String vncPassword = conn.executeStringQuery("select vnc_password from virtual_servers where server=?", virtualServer);
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
            null
        );
    }

    public static String createVirtualServer(
        DatabaseConnection conn,
        RequestSource source,
        int virtualServer
    ) throws IOException, SQLException {
        // The user must have proper permissions
        BusinessHandler.checkPermission(conn, source, "createVirtualServer", AOServPermission.Permission.control_virtual_server);
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
        BusinessHandler.checkPermission(conn, source, "rebootVirtualServer", AOServPermission.Permission.control_virtual_server);
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
        BusinessHandler.checkPermission(conn, source, "shutdownVirtualServer", AOServPermission.Permission.control_virtual_server);
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
        BusinessHandler.checkPermission(conn, source, "destroyVirtualServer", AOServPermission.Permission.control_virtual_server);
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
        BusinessHandler.checkPermission(conn, source, "pauseVirtualServer", AOServPermission.Permission.control_virtual_server);
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
        BusinessHandler.checkPermission(conn, source, "unpauseVirtualServer", AOServPermission.Permission.control_virtual_server);
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
        BusinessHandler.checkPermission(conn, source, "getVirtualServerStatus", AOServPermission.Permission.get_virtual_server_status);
        // TODO: Must not be a disabled server
        // Find current location of server
        int primaryPhysicalServer = ClusterHandler.getPrimaryPhysicalServer(virtualServer);
        // Grant access to the Xen outer server
        return DaemonHandler.getDaemonConnector(conn, primaryPhysicalServer).getVirtualServerStatus(ServerHandler.getNameForServer(conn, virtualServer));
    }
}