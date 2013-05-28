/*
 * Copyright 2001-2013 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.AOServer;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.validator.HostAddress;
import com.aoindustries.aoserv.client.validator.InetAddress;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import com.aoindustries.io.AOPool;
import com.aoindustries.sql.DatabaseAccess;
import com.aoindustries.sql.DatabaseConnection;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

/**
 * The <code>AOServDaemonHandler</code> handles all the accesses to the daemons.
 *
 * @author  AO Industries, Inc.
 */
final public class DaemonHandler {

    private static final Logger logger = LogFactory.getLogger(DaemonHandler.class);

    private DaemonHandler() {
    }

    /**
     * The amount of time before a daemon will be accessed again once
     * flagged as unavailable.
     */
    public static final int DAEMON_RETRY_DELAY=5*1000; // Used to be 60*1000

    private static final Map<Integer,AOServDaemonConnector> connectors=new HashMap<Integer,AOServDaemonConnector>();

    public static int getDaemonConcurrency() {
        int total=0;
        Iterator<Integer> I=connectors.keySet().iterator();
        while(I.hasNext()) {
            Integer key=I.next();
            total+=connectors.get(key).getConcurrency();
        }
        return total;
    }

    public static int getDaemonConnections() {
        int total=0;
        Iterator<Integer> I=connectors.keySet().iterator();
        while(I.hasNext()) {
            Integer key=I.next();
            total+=connectors.get(key).getConnectionCount();
        }
        return total;
    }

    public static HostAddress getDaemonConnectAddress(DatabaseAccess database, int aoServer) throws IOException, SQLException {
        HostAddress address = database.executeObjectQuery(
            ObjectFactories.hostAddressFactory,
            "select daemon_connect_address from ao_servers where server=?",
            aoServer
        );
        if(address!=null) return address;
        InetAddress ip = database.executeObjectQuery(
            ObjectFactories.inetAddresFactory,
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
        if(ip.isUnspecified()) {
            ip = database.executeObjectQuery(
                ObjectFactories.inetAddresFactory,
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
                + "  and nb.server=ao2.server\n"
                + "  and ao2.server=nd.server\n"
                + "  and ao2.daemon_device_id=nd.device_id\n"
                + "  and nd.pkey=ia.net_device\n"
                + "  and not ia.is_alias\n"
                + "limit 1",
                aoServer
            );
            if(ip==null) throw new SQLException("Unable to find daemon IP address for AOServer: "+aoServer);
        }
        return HostAddress.valueOf(ip);
    }

    public static int getDaemonConnectorPort(DatabaseAccess database, int aoServer) throws IOException, SQLException {
        return database.executeIntQuery(
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
    }

    public static String getDaemonConnectorProtocol(DatabaseAccess database, int aoServer) throws IOException, SQLException {
        return database.executeStringQuery(
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
    }

    public static int getDaemonConnectorPoolSize(DatabaseAccess database, int aoServer) throws IOException, SQLException {
        return database.executeIntQuery(
            "select\n"
            + "  pool_size\n"
            + "from\n"
            + "  ao_servers\n"
            + "where\n"
            + "  server=?",
            aoServer
        );
    }

    public static AOServDaemonConnector getDaemonConnector(DatabaseAccess database, int aoServer) throws IOException, SQLException {
        Integer I=Integer.valueOf(aoServer);
        synchronized(DaemonHandler.class) {
            AOServDaemonConnector O=connectors.get(I);
            if(O!=null) return O;
            AOServDaemonConnector conn=AOServDaemonConnector.getConnector(
                getDaemonConnectAddress(database, aoServer),
                MasterConfiguration.getLocalIp(),
                getDaemonConnectorPort(database, aoServer),
                getDaemonConnectorProtocol(database, aoServer),
                MasterConfiguration.getDaemonKey(database, aoServer),
                getDaemonConnectorPoolSize(database, aoServer),
                AOPool.DEFAULT_MAX_CONNECTION_AGE,
                MasterConfiguration.getSSLTruststorePath(),
                MasterConfiguration.getSSLTruststorePassword(),
                logger
            );
            connectors.put(I, conn);
            return conn;
        }
    }

    public static int getDaemonConnects() {
        int total=0;
        Iterator<Integer> I=connectors.keySet().iterator();
        while(I.hasNext()) {
            Integer key=I.next();
            total+=connectors.get(key).getConnects();
        }
        return total;
    }

    public static int getDaemonCount() {
        return connectors.size();
    }

    public static int getDaemonMaxConcurrency() {
        int total=0;
        Iterator<Integer> I=connectors.keySet().iterator();
        while(I.hasNext()) {
            Integer key=I.next();
            total+=connectors.get(key).getMaxConcurrency();
        }
        return total;
    }

    public static int getDaemonPoolSize() {
        int total=0;
        Iterator<Integer> I=connectors.keySet().iterator();
        while(I.hasNext()) {
            Integer key=I.next();
            total+=connectors.get(key).getPoolSize();
        }
        return total;
    }

    public static long getDaemonTotalTime() {
        long total=0;
        Iterator<Integer> I=connectors.keySet().iterator();
        while(I.hasNext()) {
            Integer key=I.next();
            total+=connectors.get(key).getTotalTime();
        }
        return total;
    }

    public static long getDaemonTransactions() {
        long total=0;
        Iterator<Integer> I=connectors.keySet().iterator();
        while(I.hasNext()) {
            Integer key=I.next();
            total+=connectors.get(key).getTransactionCount();
        }
        return total;
    }
    
    private static final Map<Integer,Long> downDaemons=new HashMap<Integer,Long>();

    public static void invalidateTable(SchemaTable.TableID tableID) {
        if(
            tableID==SchemaTable.TableID.AO_SERVERS
            || tableID==SchemaTable.TableID.IP_ADDRESSES
            || tableID==SchemaTable.TableID.NET_BINDS
        ) {
            synchronized(DaemonHandler.class) {
                connectors.clear();
            }
        }
    }

    /**
     * The availability of daemons is maintained to avoid repeatedly trying to access
     * a daemon that is not responding while other daemons could be used.
     */
    public static boolean isDaemonAvailable(int aoServer) {
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
    }

    public static void flagDaemonAsDown(int aoServer) throws IOException {
        Integer I=Integer.valueOf(aoServer);
        synchronized(downDaemons) {
            downDaemons.put(I, Long.valueOf(System.currentTimeMillis()));
        }
    }
    
    public static int getDownDaemonCount() {
        synchronized(downDaemons) {
            return downDaemons.size();
        }
    }
    
    private final static Map<Long,Long> recentKeys=new HashMap<Long,Long>();
    private static long lastKeyCleanTime=-1;

    /**
     * @param conn
     * @param aoServer
     * @param connectAddress Overridden connect address or <code>null</code> to use the default
     * @param daemonCommandCode
     * @param param1
     * @param param2
     * @param param3
     * @return
     * @throws java.io.IOException
     * @throws java.sql.SQLException
     */
    public static AOServer.DaemonAccess grantDaemonAccess(
        DatabaseConnection conn,
        int aoServer,
        HostAddress connectAddress,
        int daemonCommandCode,
        String param1,
        String param2,
        String param3
    ) throws IOException, SQLException {
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

        return new AOServer.DaemonAccess(
            getDaemonConnectorProtocol(conn, aoServer),
            connectAddress!=null ? connectAddress : getDaemonConnectAddress(conn, aoServer),
            getDaemonConnectorPort(conn, aoServer),
            key
        );
    }
}