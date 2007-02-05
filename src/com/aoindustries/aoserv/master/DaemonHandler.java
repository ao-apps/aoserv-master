package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.daemon.client.*;
import com.aoindustries.io.*;
import com.aoindustries.profiler.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * The <code>AOServDaemonHandler</code> handles all the accesses to the daemons.
 *
 * @author  AO Industries, Inc.
 */
final public class DaemonHandler {

    /**
     * The amount of time before a daemon will be accessed again once
     * flagged as unavailable.
     */
    public static final int DAEMON_RETRY_DELAY=60*1000;

    private static final Map<Integer,AOServDaemonConnector> connectors=new HashMap<Integer,AOServDaemonConnector>();

    public static int getDaemonConcurrency() {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "getDaemonConcurrency()", null);
        try {
            int total=0;
            Iterator<Integer> I=connectors.keySet().iterator();
            while(I.hasNext()) {
                Integer key=I.next();
                total+=connectors.get(key).getConcurrency();
            }
            return total;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static int getDaemonConnections() {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "getDaemonConnections()", null);
        try {
            int total=0;
            Iterator<Integer> I=connectors.keySet().iterator();
            while(I.hasNext()) {
                Integer key=I.next();
                total+=connectors.get(key).getConnectionCount();
            }
            return total;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getDaemonConnectorIP(MasterDatabaseConnection dbConn, int aoServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "getDaemonConnectorIP(MasterDatabaseConnection,int)", null);
        try {
            String address=dbConn.executeStringQuery("select daemon_connect_address from ao_servers where server=?", aoServer);
            if(address!=null) return address;
            String ip=dbConn.executeStringQuery(
                "select\n"
                + "  ia.ip_address\n"
                + "from\n"
                + "  ao_servers ao,\n"
                + "  net_binds nb,\n"
                + "  ip_addresses ia\n"
                + "where\n"
                + "  ao.server=?\n"
                + "  and ao.daemon_connect_bind=nb.pkey\n"
                + "  and nb.ip_address=ia.pkey",
                aoServer
            );
            if(ip==null) throw new SQLException("Unable to find daemon IP address for AOServer: "+aoServer);
            if(ip.equals(IPAddress.WILDCARD_IP)) {
                ip=dbConn.executeStringQuery(
                    "select\n"
                    + "  ia.ip_address\n"
                    + "from\n"
                    + "  ao_servers ao,\n"
                    + "  net_binds nb,\n"
                    + "  ao_servers ao2,\n"
                    + "  net_devices nd,\n"
                    + "  ip_addresses ia\n"
                    + "where\n"
                    + "  ao.server=?\n"
                    + "  and ao.daemon_connect_bind=nb.pkey\n"
                    + "  and nb.ao_server=ao2.server\n"
                    + "  and ao2.server=nd.ao_server\n"
                    + "  and ao2.daemon_device_id=nd.device_id\n"
                    + "  and nd.pkey=ia.net_device\n"
                    + "  and not ia.is_alias\n"
                    + "limit 1",
                    aoServer
                );
                if(ip==null) throw new SQLException("Unable to find daemon IP address for AOServer: "+aoServer);
            }
            return ip;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static int getDaemonConnectorPort(MasterDatabaseConnection conn, int aoServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "getDaemonConnectorPort(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery(
                "select\n"
                + "  nb.port\n"
                + "from\n"
                + "  ao_servers ao,\n"
                + "  net_binds nb\n"
                + "where\n"
                + "  ao.server=?\n"
                + "  and ao.daemon_connect_bind=nb.pkey",
                aoServer
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getDaemonConnectorProtocol(MasterDatabaseConnection conn, int aoServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "getDaemonConnectorProtocol(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery(
                "select\n"
                + "  nb.app_protocol\n"
                + "from\n"
                + "  ao_servers ao,\n"
                + "  net_binds nb\n"
                + "where\n"
                + "  ao.server=?\n"
                + "  and ao.daemon_connect_bind=nb.pkey",
                aoServer
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static int getDaemonConnectorPoolSize(MasterDatabaseConnection conn, int aoServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "getDaemonConnectorPoolSize(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery(
                "select\n"
                + "  pool_size\n"
                + "from\n"
                + "  ao_servers\n"
                + "where\n"
                + "  server=?",
                aoServer
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static AOServDaemonConnector getDaemonConnector(MasterDatabaseConnection dbConn, int aoServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "getDaemonConnector(MasterDatabaseConnection,int)", Integer.valueOf(aoServer));
        try {
            Integer I=Integer.valueOf(aoServer);
	    synchronized(DaemonHandler.class) {
		AOServDaemonConnector O=connectors.get(I);
		if(O!=null) return O;
                String ip=getDaemonConnectorIP(dbConn, aoServer);

		AOServDaemonConnector conn=AOServDaemonConnector.getConnector(
                    aoServer,
                    ip,
                    MasterConfiguration.getLocalIp(),
                    getDaemonConnectorPort(dbConn, aoServer),
                    getDaemonConnectorProtocol(dbConn, aoServer),
                    MasterConfiguration.getDaemonKey(dbConn, aoServer),
                    getDaemonConnectorPoolSize(dbConn, aoServer),
                    AOPool.DEFAULT_MAX_CONNECTION_AGE,
                    SSLServer.class,
                    SSLServer.sslProviderLoaded,
                    MasterConfiguration.getSSLTruststorePath(),
                    MasterConfiguration.getSSLTruststorePassword(),
                    MasterServer.getErrorHandler()
                );
		connectors.put(I, conn);
		return conn;
	    }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static int getDaemonConnects() {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "getDaemonConnects()", null);
        try {
            int total=0;
            Iterator<Integer> I=connectors.keySet().iterator();
            while(I.hasNext()) {
                Integer key=I.next();
                total+=connectors.get(key).getConnects();
            }
            return total;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static int getDaemonCount() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, DaemonHandler.class, "getDaemonCount()", null);
        try {
            return connectors.size();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public static int getDaemonMaxConcurrency() {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "getDaemonMaxConcurrency()", null);
        try {
            int total=0;
            Iterator<Integer> I=connectors.keySet().iterator();
            while(I.hasNext()) {
                Integer key=I.next();
                total+=connectors.get(key).getMaxConcurrency();
            }
            return total;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static int getDaemonPoolSize() {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "getDaemonPoolSize()", null);
        try {
            int total=0;
            Iterator<Integer> I=connectors.keySet().iterator();
            while(I.hasNext()) {
                Integer key=I.next();
                total+=connectors.get(key).getPoolSize();
            }
            return total;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static long getDaemonTotalTime() {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "getDaemonTotalTime()", null);
        try {
            long total=0;
            Iterator<Integer> I=connectors.keySet().iterator();
            while(I.hasNext()) {
                Integer key=I.next();
                total+=connectors.get(key).getTotalTime();
            }
            return total;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static long getDaemonTransactions() {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "getDaemonTransactions()", null);
        try {
            long total=0;
            Iterator<Integer> I=connectors.keySet().iterator();
            while(I.hasNext()) {
                Integer key=I.next();
                total+=connectors.get(key).getTransactionCount();
            }
            return total;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    private static final Map<Integer,Long> downDaemons=new HashMap<Integer,Long>();

    public static void invalidateTable(int tableID) {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "invalidateTable(int)", null);
        try {
            if(
                tableID==SchemaTable.AO_SERVERS
                || tableID==SchemaTable.IP_ADDRESSES
                || tableID==SchemaTable.NET_BINDS
            ) {
                synchronized(DaemonHandler.class) {
                    connectors.clear();
                }
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * The availability of daemons is maintained to avoid repeatedly trying to access
     * a daemon that is not responding while other daemons could be used.
     */
    public static boolean isDaemonAvailable(int aoServer) {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "isDaemonAvailable(int)", null);
        try {
            Integer I=Integer.valueOf(aoServer);
            synchronized(downDaemons) {
                Long O=downDaemons.get(I);
                if(O!=null) {
                    long downTime=System.currentTimeMillis()-O.longValue();
                    if(downTime<0) {
                        downDaemons.remove(I);
                        return true;
                    }
                    if(downTime<DAEMON_RETRY_DELAY) return false;
                    downDaemons.remove(I);
                }
            }
            return true;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void flagDaemonAsDown(int aoServer) throws IOException {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "flagDaemonAsDown(int)", null);
        try {
            Integer I=Integer.valueOf(aoServer);
            synchronized(downDaemons) {
                downDaemons.put(I, Long.valueOf(System.currentTimeMillis()));
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    public static int getDownDaemonCount() {
        Profiler.startProfile(Profiler.FAST, DaemonHandler.class, "getDownDaemonCount()", null);
        try {
            synchronized(downDaemons) {
                return downDaemons.size();
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    private final static Map<Long,Long> recentKeys=new HashMap<Long,Long>();
    private static long lastKeyCleanTime=-1;

    public static long requestDaemonAccess(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer,
        int daemonCommandCode,
        int param1
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DaemonHandler.class, "requestDaemonAccess(MasterDatabaseConnection,RequestSource,int,int,int)", null);
        try {
            // Security checks
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            if(masterUser==null) throw new SQLException("Only master users allowed to request daemon access.");
            if(daemonCommandCode==AOServDaemonProtocol.FAILOVER_FILE_REPLICATION) {
                // Current user must be able to access the from server
                int fromServer=FailoverHandler.getFromAOServerForFailoverFileReplication(conn, param1);
                ServerHandler.checkAccessServer(conn, source, "requestDaemonAccess", fromServer);

                // The to server must match server
                int toServer=FailoverHandler.getToAOServerForFailoverFileReplication(conn, param1);
                if(toServer!=aoServer) throw new SQLException("(ao_servers.server="+aoServer+")!=((failover_file_replication.pkey="+param1+").to_server="+toServer+")");

                // Chunk always will be automatically performed once a month between the 2nd and the 28th, based on mod of ffr.pkey equaling the current day of the month
                boolean chunkAlways = Calendar.getInstance().get(Calendar.DAY_OF_MONTH)==((param1%27)+2);
                if(!chunkAlways) chunkAlways = conn.executeBooleanQuery("select chunk_always from failover_file_replications where pkey=?", param1);
                return grantDaemonAccess(
                    conn,
                    aoServer,
                    AOServDaemonProtocol.FAILOVER_FILE_REPLICATION,
                    ServerHandler.getHostnameForServer(conn, fromServer),
                    conn.executeStringQuery("select to_path from failover_file_replications where pkey=?", param1),
                    chunkAlways ? "t" : "f"
                );
            } else throw new SQLException("Unknown daemon command code: "+daemonCommandCode);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static long grantDaemonAccess(
        MasterDatabaseConnection conn,
        int aoServer,
        int daemonCommandCode,
        String param1,
        String param2,
        String param3
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DaemonHandler.class, "grantDaemonAccess(MasterDatabaseConnection,int,int,String,String,String)", null);
        try {
            long key;
            synchronized(recentKeys) {
                long currentTime=System.currentTimeMillis();
                if(lastKeyCleanTime==-1) lastKeyCleanTime=currentTime;
                else {
                    long timeSince=currentTime-lastKeyCleanTime;
                    if(timeSince<0 || timeSince>=(5L*60*1000)) {
                        // Clean up the entries over one hour old
                        Iterator<Long> I=recentKeys.keySet().iterator();
                        while(I.hasNext()) {
                            Long keyObj=I.next();
                            long time=recentKeys.get(keyObj).longValue();
                            timeSince=currentTime-time;
                            if(timeSince<0 || timeSince>=(60L*60*1000)) {
                                I.remove();
                            }
                        }
                        lastKeyCleanTime=currentTime;
                    }
                }
                
                // Generate the key
                Random random=MasterServer.getRandom();
                while(true) {
                    key=random.nextLong();
                    Long L=Long.valueOf(key);
                    if(!recentKeys.containsKey(L)) {
                        recentKeys.put(L, Long.valueOf(System.currentTimeMillis()));
                        break;
                    }
                }
            }                

            // Send the key to the daemon
            getDaemonConnector(conn, aoServer).grantDaemonAccess(key, daemonCommandCode, param1, param2, param3);

            return key;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}