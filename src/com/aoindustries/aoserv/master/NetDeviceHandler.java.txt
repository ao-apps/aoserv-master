package com.aoindustries.aoserv.master;

/*
 * Copyright 2008-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.sql.DatabaseConnection;
import java.io.IOException;
import java.sql.SQLException;

/**
 * The <code>NetDeviceHandler</code> handles all the accesses to the <code>net_devices</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class NetDeviceHandler {

    public static String getNetDeviceBondingReport(DatabaseConnection conn, RequestSource source, int pkey) throws IOException, SQLException {
        int server = getServerForNetDevice(conn, pkey);
        if(!ServerHandler.isAOServer(conn, server)) throw new SQLException("Server is not an AOServer: "+server);
        ServerHandler.checkAccessServer(conn, source, "getNetDeviceBondingReport", server);

        return DaemonHandler.getDaemonConnector(conn, server).getNetDeviceBondingReport(pkey);
    }

    public static String getNetDeviceStatisticsReport(DatabaseConnection conn, RequestSource source, int pkey) throws IOException, SQLException {
        int server = getServerForNetDevice(conn, pkey);
        if(!ServerHandler.isAOServer(conn, server)) throw new SQLException("Server is not an AOServer: "+server);
        ServerHandler.checkAccessServer(conn, source, "getNetDeviceStatisticsReport", server);

        return DaemonHandler.getDaemonConnector(conn, server).getNetDeviceStatisticsReport(pkey);
    }

    public static int getServerForNetDevice(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeIntQuery("select server from net_devices where pkey=?", pkey);
    }
}
