package com.aoindustries.aoserv.master;

/*
 * Copyright 2008-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.profiler.Profiler;
import java.io.IOException;
import java.sql.SQLException;

/**
 * The <code>NetDeviceHandler</code> handles all the accesses to the <code>net_devices</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class NetDeviceHandler {

    public static String getNetDeviceBondingReport(MasterDatabaseConnection conn, RequestSource source, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, NetDeviceHandler.class, "getNetDeviceBondingReport(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            int server = getServerForNetDevice(conn, pkey);
            if(!ServerHandler.isAOServer(conn, server)) throw new SQLException("Server is not an AOServer: "+server);
            ServerHandler.checkAccessServer(conn, source, "getNetDeviceBondingReport", server);

            return DaemonHandler.getDaemonConnector(conn, server).getNetDeviceBondingReport(pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getNetDeviceStatisticsReport(MasterDatabaseConnection conn, RequestSource source, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, NetDeviceHandler.class, "getNetDeviceStatisticsReport(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            int server = getServerForNetDevice(conn, pkey);
            if(!ServerHandler.isAOServer(conn, server)) throw new SQLException("Server is not an AOServer: "+server);
            ServerHandler.checkAccessServer(conn, source, "getNetDeviceStatisticsReport", server);

            return DaemonHandler.getDaemonConnector(conn, server).getNetDeviceStatisticsReport(pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getServerForNetDevice(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, NetDeviceHandler.class, "getServerForNetDevice(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select server from net_devices where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}
