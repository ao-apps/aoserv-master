package com.aoindustries.aoserv.master;

/*
 * Copyright 2003-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.io.*;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * The <code>AOServerHandler</code> handles all the accesses to the ao_servers table.
 *
 * @author  AO Industries, Inc.
 */
final public class AOServerHandler {

    public static IntList getAOServers(MasterDatabaseConnection conn) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, AOServerHandler.class, "getAOServers(MasterDatabaseConnection)", null);
        try {
            return conn.executeIntListQuery(Connection.TRANSACTION_READ_COMMITTED, true, "select server from ao_servers");
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private static final Map<Integer,Object> mrtgLocks = new HashMap<Integer,Object>();

    public static void getMrtgFile(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer,
        String filename,
        CompressedDataOutputStream out
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, AOServerHandler.class, "getMrtgFile(MasterDatabaseConnection,RequestSource,int,String,CompressedDataOutputStream)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setLastDistroTime(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int aoServer,
        long time
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, AOServerHandler.class, "setLastDistroTime(MasterDatabaseConnection,RequestSource,InvalidateList,int,long)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void startDistro(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer,
        boolean includeUser
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, AOServerHandler.class, "startDistro(MasterDatabaseConnection,RequestSource,int,boolean)", null);
        try {
            ServerHandler.checkAccessServer(conn, source, "startDistro", aoServer);
            MasterUser mu=MasterServer.getMasterUser(conn, source.getUsername());
            if(mu==null) throw new SQLException("Only master users may start distribution verifications: "+source.getUsername());
            ServerHandler.checkAccessServer(conn, source, "startDistro", aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).startDistro(includeUser);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void restartCron(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, AOServerHandler.class, "restartCron(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "cron");
            if(!canControl) throw new SQLException("Not allowed to restart Cron on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).restartCron();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void startCron(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, AOServerHandler.class, "startCron(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "cron");
            if(!canControl) throw new SQLException("Not allowed to start Cron on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).startCron();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void stopCron(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, AOServerHandler.class, "stopCron(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "cron");
            if(!canControl) throw new SQLException("Not allowed to stop Cron on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).stopCron();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void restartXfs(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, AOServerHandler.class, "restartXfs(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "xfs");
            if(!canControl) throw new SQLException("Not allowed to restart XFS on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).restartXfs();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void startXfs(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, AOServerHandler.class, "startXfs(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "xfs");
            if(!canControl) throw new SQLException("Not allowed to start XFS on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).startXfs();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void stopXfs(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, AOServerHandler.class, "stopXfs(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "xfs");
            if(!canControl) throw new SQLException("Not allowed to stop XFS on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).stopXfs();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void restartXvfb(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, AOServerHandler.class, "restartXvfb(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "xvfb");
            if(!canControl) throw new SQLException("Not allowed to restart Xvfb on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).restartXvfb();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void startXvfb(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, AOServerHandler.class, "startXvfb(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "xvfb");
            if(!canControl) throw new SQLException("Not allowed to start Xvfb on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).startXvfb();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void stopXvfb(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, AOServerHandler.class, "stopXvfb(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "xvfb");
            if(!canControl) throw new SQLException("Not allowed to stop Xvfb on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).stopXvfb();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}