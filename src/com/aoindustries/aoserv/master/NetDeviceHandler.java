package com.aoindustries.aoserv.master;

/*
 * Copyright 2008 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
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

    /**
     * Gets the contents of a user cron table.
     */
    public static String getNetDeviceBondingReport(MasterDatabaseConnection conn, RequestSource source, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, NetDeviceHandler.class, "getNetDeviceBondingReport(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            int aoServer = getAOServerForNetDevice(conn, pkey);
            ServerHandler.checkAccessServer(conn, source, "getNetDeviceBondingReport", aoServer);

            return DaemonHandler.getDaemonConnector(conn, aoServer).getNetDeviceBondingReport(pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForNetDevice(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, NetDeviceHandler.class, "getAOServerForNetDevice(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select ao_server from net_devices where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}
