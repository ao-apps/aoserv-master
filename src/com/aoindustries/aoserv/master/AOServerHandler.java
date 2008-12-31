package com.aoindustries.aoserv.master;

/*
 * Copyright 2003-2008 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.util.IntList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * The <code>AOServerHandler</code> handles all the accesses to the ao_servers table.
 *
 * @author  AO Industries, Inc.
 */
final public class AOServerHandler {

    public static IntList getAOServers(MasterDatabaseConnection conn) throws IOException, SQLException {
        return conn.executeIntListQuery(Connection.TRANSACTION_READ_COMMITTED, true, "select server from ao_servers");
    }

    private static final Map<Integer,Object> mrtgLocks = new HashMap<Integer,Object>();

    public static void getMrtgFile(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer,
        String filename,
        CompressedDataOutputStream out
    ) throws IOException, SQLException {
        ServerHandler.checkAccessServer(conn, source, "getMrtgFile", aoServer);
        if(filename.indexOf('/')!=-1 || filename.indexOf("..")!=-1) throw new SQLException("Invalidate filename: "+filename);

        // Only one MRTG graph per server at a time, if don't get the lock in 15 seconds, return an error
        synchronized(mrtgLocks) {
            long startTime = System.currentTimeMillis();
            do {
                if(mrtgLocks.containsKey(aoServer)) {
                    long currentTime = System.currentTimeMillis();
                    if(startTime > currentTime) startTime = currentTime;
                    else if((currentTime - startTime)>=15000) throw new IOException("15 second timeout reached while trying to get lock to access server #"+aoServer);
                    else {
                        try {
                            mrtgLocks.wait(startTime + 15000 - currentTime);
                        } catch(InterruptedException err) {
                            MasterServer.reportWarning(err, null);
                        }
                    }
                }
            } while(mrtgLocks.containsKey(aoServer));
            mrtgLocks.put(aoServer, Boolean.TRUE);
            mrtgLocks.notifyAll();
        }
        try {
            if(DaemonHandler.isDaemonAvailable(aoServer)) {
                try {
                    DaemonHandler.getDaemonConnector(conn, aoServer).getMrtgFile(filename, out);
                } catch(IOException err) {
                    DaemonHandler.flagDaemonAsDown(aoServer);
                    IOException newErr = new IOException("Server Unavailable");
                    newErr.initCause(err);
                    throw newErr;
                }
            } else throw new IOException("Server Unavailable");
        } finally {
            synchronized(mrtgLocks) {
                mrtgLocks.remove(aoServer);
                mrtgLocks.notifyAll();
            }
        }
    }

    public static void setLastDistroTime(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int aoServer,
        long time
    ) throws IOException, SQLException {
        String mustring = source.getUsername();
        MasterUser mu = MasterServer.getMasterUser(conn, mustring);
        if (mu==null) throw new SQLException("User "+mustring+" is not master user and may not set the last distro time");
        ServerHandler.checkAccessServer(conn, source, "setLastDistroTime", aoServer);
        conn.executeUpdate(
            "update ao_servers set last_distro_time=? where server=?",
            new Timestamp(time),
            aoServer
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.SERVERS,
            ServerHandler.getBusinessesForServer(conn, aoServer),
            aoServer,
            false
        );
    }

    public static void startDistro(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer,
        boolean includeUser
    ) throws IOException, SQLException {
        ServerHandler.checkAccessServer(conn, source, "startDistro", aoServer);
        MasterUser mu=MasterServer.getMasterUser(conn, source.getUsername());
        if(mu==null) throw new SQLException("Only master users may start distribution verifications: "+source.getUsername());
        ServerHandler.checkAccessServer(conn, source, "startDistro", aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).startDistro(includeUser);
    }

    public static void restartCron(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "cron");
        if(!canControl) throw new SQLException("Not allowed to restart Cron on "+aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).restartCron();
    }

    public static void startCron(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "cron");
        if(!canControl) throw new SQLException("Not allowed to start Cron on "+aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).startCron();
    }

    public static void stopCron(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "cron");
        if(!canControl) throw new SQLException("Not allowed to stop Cron on "+aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).stopCron();
    }

    public static void restartXfs(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "xfs");
        if(!canControl) throw new SQLException("Not allowed to restart XFS on "+aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).restartXfs();
    }

    public static void startXfs(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "xfs");
        if(!canControl) throw new SQLException("Not allowed to start XFS on "+aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).startXfs();
    }

    public static void stopXfs(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "xfs");
        if(!canControl) throw new SQLException("Not allowed to stop XFS on "+aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).stopXfs();
    }

    public static void restartXvfb(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "xvfb");
        if(!canControl) throw new SQLException("Not allowed to restart Xvfb on "+aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).restartXvfb();
    }

    public static void startXvfb(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "xvfb");
        if(!canControl) throw new SQLException("Not allowed to start Xvfb on "+aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).startXvfb();
    }

    public static void stopXvfb(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "xvfb");
        if(!canControl) throw new SQLException("Not allowed to stop Xvfb on "+aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).stopXvfb();
    }

    public static String get3wareRaidReport(MasterDatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
        ServerHandler.checkAccessServer(conn, source, "get3wareRaidReport", aoServer);

        return DaemonHandler.getDaemonConnector(conn, aoServer).get3wareRaidReport();
    }

    public static String getMdRaidReport(MasterDatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
        ServerHandler.checkAccessServer(conn, source, "getMdRaidReport", aoServer);

        return DaemonHandler.getDaemonConnector(conn, aoServer).getMdRaidReport();
    }

    public static String getDrbdReport(MasterDatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
        ServerHandler.checkAccessServer(conn, source, "getDrbdReport", aoServer);

        return DaemonHandler.getDaemonConnector(conn, aoServer).getDrbdReport();
    }

    public static String getHddTempReport(MasterDatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
        ServerHandler.checkAccessServer(conn, source, "getHddTempReport", aoServer);

        return DaemonHandler.getDaemonConnector(conn, aoServer).getHddTempReport();
    }

    public static String getFilesystemsCsvReport(MasterDatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
        ServerHandler.checkAccessServer(conn, source, "getFilesystemsCsvReport", aoServer);

        return DaemonHandler.getDaemonConnector(conn, aoServer).getFilesystemsCsvReport();
    }

    public static String getLoadAvgReport(MasterDatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
        ServerHandler.checkAccessServer(conn, source, "getLoadAvgReport", aoServer);

        return DaemonHandler.getDaemonConnector(conn, aoServer).getLoadAvgReport();
    }

    public static String getMemInfoReport(MasterDatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
        ServerHandler.checkAccessServer(conn, source, "getMemInfoReport", aoServer);

        return DaemonHandler.getDaemonConnector(conn, aoServer).getMemInfoReport();
    }

    public static long getSystemTimeMillis(MasterDatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
        ServerHandler.checkAccessServer(conn, source, "getSystemTimeMillis", aoServer);

        return DaemonHandler.getDaemonConnector(conn, aoServer).getSystemTimeMillis();
    }
}
