/*
 * Copyright 2001-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import com.aoindustries.io.AOPool;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.EnumSet;
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

    private static final Random random = new SecureRandom();

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

    public static AOServDaemonConnector getDaemonConnector(AOServer aoServer) throws IOException {
        try {
            Integer I=aoServer.getKey();
            synchronized(DaemonHandler.class) {
                AOServDaemonConnector O=connectors.get(I);
                if(O!=null) return O;
                NetBind daemonConnectBind = aoServer.getDaemonConnectBind();
                AOServDaemonConnector conn=AOServDaemonConnector.getConnector(
                    Hostname.valueOf(aoServer.getDaemonConnectorIP()),
                    MasterConfiguration.getLocalIp(),
                    daemonConnectBind.getPort(),
                    daemonConnectBind.getAppProtocol().getName(),
                    MasterConfiguration.getDaemonKey(aoServer),
                    aoServer.getPoolSize(),
                    AOPool.DEFAULT_MAX_CONNECTION_AGE,
                    MasterConfiguration.getSSLTruststorePath(),
                    MasterConfiguration.getSSLTruststorePassword(),
                    logger
                );
                connectors.put(I, conn);
                return conn;
            }
        } catch(ValidationException err) {
            throw new IOException(err);
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

    public static void invalidateServices(EnumSet<ServiceName> services) {
        if(
            services.contains(ServiceName.ao_servers)
            || services.contains(ServiceName.ip_addresses)
            || services.contains(ServiceName.net_binds)
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
     */
    public static AOServer.DaemonAccess grantDaemonAccess(
        AOServer aoServer,
        InetAddress connectAddress,
        int daemonCommandCode,
        String param1,
        String param2,
        String param3
    ) throws IOException {
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
        getDaemonConnector(aoServer).grantDaemonAccess(key, daemonCommandCode, param1, param2, param3);

        NetBind daemonConnectBind = aoServer.getDaemonConnectBind();
        return new AOServer.DaemonAccess(
            daemonConnectBind.getAppProtocol().getProtocol(),
            Hostname.valueOf(connectAddress!=null ? connectAddress : aoServer.getDaemonConnectorIP()),
            daemonConnectBind.getPort(),
            key
        );
    }
}