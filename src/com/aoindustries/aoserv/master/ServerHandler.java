package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.profiler.Profiler;
import com.aoindustries.util.IntList;
import com.aoindustries.util.LongArrayList;
import com.aoindustries.util.LongList;
import com.aoindustries.util.SortedIntArrayList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The <code>ServerHandler</code> handles all the accesses to the Server tables.
 *
 * @author  AO Industries, Inc.
 */
final public class ServerHandler {

    private static Map<String,List<Integer>> usernameServers;

    /*
    public static int addBackupServer(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String hostname,
        String farm,
        int owner,
        String description,
        int os_version,
        String username,
        String password,
        String contact_phone,
        String contact_email
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, ServerHandler.class, "addBackupServer(MasterDatabaseConnection,RequestSource,InvalidateList,String,String,int,String,int,String,String,String,String)", null);
        try {
            // Security and validity checks
            String accounting=UsernameHandler.getBusinessForUsername(conn, source.getUsername());
            if(
                !conn.executeBooleanQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select can_add_backup_server from businesses where accounting=?", accounting)
            ) throw new SQLException("Not allowed to add_backup_server: "+source.getUsername());

            MasterServer.checkAccessHostname(conn, source, "addBackupServer", hostname);

            String farm_owner=conn.executeStringQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                true,
                "select\n"
                + "  pk.accounting\n"
                + "from\n"
                + "  server_farms sf,\n"
                + "  packages pk\n"
                + "where\n"
                + "  sf.name=?\n"
                + "  and sf.owner=pk.pkey",
                farm
            );
            if(!BusinessHandler.isBusinessOrParent(conn, accounting, farm_owner)) throw new SQLException("Not able to access farm: "+farm);

            PackageHandler.checkAccessPackage(conn, source, "addBackupServer", owner);

            String check = Username.checkUsername(username, Locale.getDefault());
            if(check!=null) throw new SQLException(check);
            
            PasswordChecker.Result[] results = BusinessAdministrator.checkPassword(Locale.getDefault(), username, password);
            if(PasswordChecker.hasResults(Locale.getDefault(), results)) throw new SQLException("Password strength check failed: "+PasswordChecker.getResultsString(results).replace('\n', '|'));
            
            int serverPKey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('servers_pkey_seq')");
            conn.executeUpdate(
                "insert into\n"
                + "  servers\n"
                + "values(\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  'orion',\n"
                + "  ?,\n"
                + "  null,\n"
                + "  ?,\n"
                + "  null\n"
                + ")",
                serverPKey,
                hostname,
                farm,
                PackageHandler.getBusinessForPackage(conn, owner),
                description,
                os_version
            );
            invalidateList.addTable(conn, SchemaTable.TableID.SERVERS, accounting, InvalidateList.allServers, false);

            // Build a stack of parents, adding each business_server
            Stack<String> bus=new Stack<String>();
            String packageAccounting=PackageHandler.getBusinessForPackage(conn, owner);
            String currentAccounting=packageAccounting;
            while(true) {
                bus.push(currentAccounting);
                if(currentAccounting.equals(BusinessHandler.getRootBusiness())) break;
                currentAccounting=BusinessHandler.getParentBusiness(conn, currentAccounting);
            }
            while(!bus.isEmpty()) {
                BusinessHandler.addBusinessServer(conn, invalidateList, bus.pop(), serverPKey, true);
            }

            UsernameHandler.addUsername(conn, source, invalidateList, PackageHandler.getNameForPackage(conn, owner), username, false);
            
            BusinessHandler.addBusinessAdministrator(
                conn,
                source,
                invalidateList,
                username,
                hostname+" backup",
                null,
                -1,
                true,
                contact_phone,
                null,
                null,
                null,
                contact_email,
                null,
                null,
                null,
                null,
                null,
                null
            );
            conn.executeUpdate("insert into master_users values(?,true,false,false,false,false,false,false)", username);
            invalidateList.addTable(conn, SchemaTable.TableID.MASTER_USERS, packageAccounting, InvalidateList.allServers, false);
            conn.executeUpdate("insert into master_servers(username, server) values(?,?)", username, serverPKey);
            invalidateList.addTable(conn, SchemaTable.TableID.MASTER_SERVERS, packageAccounting, InvalidateList.allServers, false);
            BusinessHandler.setBusinessAdministratorPassword(conn, source, invalidateList, username, password);
            
            return serverPKey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }*/

    /*public static void checkAccessServer(MasterDatabaseConnection conn, RequestSource source, String action, String server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, ServerHandler.class, "checkAccessServer(MasterDatabaseConnection,RequestSource,String,String)", null);
        try {
            if(!canAccessServer(conn, source, server)) {
                String message=
                    "business_administrator.username="
                    +source.getUsername()
                    +" is not allowed to access server: action='"
                    +action
                    +", hostname="
                    +server
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }*/

    public static void checkAccessServer(MasterDatabaseConnection conn, RequestSource source, String action, int server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, ServerHandler.class, "checkAccessServer(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            if(!canAccessServer(conn, source, server)) {
                String message=
                    "business_administrator.username="
                    +source.getUsername()
                    +" is not allowed to access server: action='"
                    +action
                    +", server.pkey="
                    +server
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /*
    public static boolean canAccessServer(MasterDatabaseConnection conn, RequestSource source, String server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, ServerHandler.class, "canAccessServer(MasterDatabaseConnection,RequestSource,String)", null);
        try {
            return getAllowedServers(conn, source).contains(server);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }*/

    public static boolean canAccessServer(MasterDatabaseConnection conn, RequestSource source, int server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, ServerHandler.class, "canAccessServer(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            return getAllowedServers(conn, source).contains(server);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Creates a new <code>CreditCard</code>.
     */
    public static int addServerReport(
        MasterDatabaseConnection conn,
        RequestSource source,
        String server,
        long time,
        long interval,
        float[][] values
    ) throws IOException {
        Profiler.startProfile(Profiler.FAST, ServerHandler.class, "addServerReport(MasterDatabaseConnection,RequestSource,String,long,long,float[][])", null);
        try {
            throw new IOException("TODO: Not implemented");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Gets the servers that are allowed for the provided username.
     */
    static List<Integer> getAllowedServers(MasterDatabaseConnection conn, RequestSource source) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, ServerHandler.class, "getAllowedServers(MasterDatabaseConnection,RequestSource)", null);
        try {
	    synchronized(ServerHandler.class) {
		String username=source.getUsername();
		if(usernameServers==null) usernameServers=new HashMap<String,List<Integer>>();
		List<Integer> SV=usernameServers.get(username);
		if(SV==null) {
		    SV=new SortedIntArrayList();
                    MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
                    if(mu!=null) {
                        com.aoindustries.aoserv.client.MasterServer[] masterServers = MasterServer.getMasterServers(conn, source.getUsername());
                        if(masterServers.length!=0) {
                            for(com.aoindustries.aoserv.client.MasterServer masterServer : masterServers) {
                                SV.add(masterServer.getServerPKey());
                            }
                        } else {
                            SV.addAll(conn.executeIntListQuery("select pkey from servers"));
                        }
                    } else {
                        SV.addAll(
                            conn.executeIntListQuery(
                                "select\n"
                                + "  bs.server\n"
                                + "from\n"
                                + "  usernames un,\n"
                                + "  packages pk,\n"
                                + "  business_servers bs\n"
                                + "where\n"
                                + "  un.username=?\n"
                                + "  and un.package=pk.name\n"
                                + "  and pk.accounting=bs.accounting",
                                username
                            )
                        );
                    }
		    usernameServers.put(username, SV);
		}
		return SV;
	    }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static List<String> getBusinessesForServer(MasterDatabaseConnection conn, int server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, ServerHandler.class, "getBusinessesForServer(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringListQuery("select accounting from business_servers where server=?", server);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private static Map<Integer,Integer> failoverServers=new HashMap<Integer,Integer>();
    public static int getFailoverServer(MasterDatabaseConnection conn, int aoServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, ServerHandler.class, "getFailoverServer(MasterDatabaseConnection,int)", null);
        try {
            synchronized(failoverServers) {
                if(failoverServers.containsKey(aoServer)) return failoverServers.get(aoServer).intValue();
                int failoverServer=conn.executeIntQuery(
                    "select\n"
                    + "  coalesce(\n"
                    + "    (\n"
                    + "      select\n"
                    + "        failover_server\n"
                    + "      from\n"
                    + "        ao_servers\n"
                    + "      where\n"
                    + "        server=?\n"
                    + "    ), -1\n"
                    + "  )",
                    aoServer
                );
                failoverServers.put(aoServer, failoverServer);
                return failoverServer;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private static Map<Integer,String> farmsForServers=new HashMap<Integer,String>();
    public static String getFarmForServer(MasterDatabaseConnection conn, int server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, ServerHandler.class, "getFarmForServer(MasterDatabaseConnection,int)", null);
        try {
            Integer I=Integer.valueOf(server);
            synchronized(farmsForServers) {
                String farm=farmsForServers.get(I);
                if(farm==null) {
                    farm=conn.executeStringQuery("select farm from servers where pkey=?", server);
                    farmsForServers.put(I, farm);
                }
                return farm;
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private static Map<Integer,String> hostnamesForAOServers=new HashMap<Integer,String>();
    public static String getHostnameForAOServer(MasterDatabaseConnection conn, int aoServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, ServerHandler.class, "getHostnameForAOServer(MasterDatabaseConnection,int)", null);
        try {
            Integer I=Integer.valueOf(aoServer);
            synchronized(hostnamesForAOServers) {
                String hostname=hostnamesForAOServers.get(I);
                if(hostname==null) {
                    hostname=conn.executeStringQuery("select hostname from ao_servers where server=?", aoServer);
                    hostnamesForAOServers.put(I, hostname);
                }
                return hostname;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Gets the operating system version for a server or <code>-1</code> if not available.
     */
    public static int getOperatingSystemVersionForServer(MasterDatabaseConnection conn, int server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, ServerHandler.class, "getOperatingSystemVersionForServer(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select coalesce((select operating_system_version from servers where pkey=?), -1)", server);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private static Map<String,Integer> serversForAOServers=new HashMap<String,Integer>();
    public static int getServerForAOServerHostname(MasterDatabaseConnection conn, String aoServerHostname) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, ServerHandler.class, "getServerForAOServerHostname(MasterDatabaseConnection,String)", null);
        try {
            synchronized(serversForAOServers) {
                Integer I=serversForAOServers.get(aoServerHostname);
                int server;
                if(I==null) {
                    server=conn.executeIntQuery("select server from ao_servers where hostname=?", aoServerHostname);
                    serversForAOServers.put(aoServerHostname, server);
                } else server=I.intValue();
                return server;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static IntList getServers(MasterDatabaseConnection conn) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, ServerHandler.class, "getServers(MasterDatabaseConnection)", null);
        try {
            return conn.executeIntListQuery(Connection.TRANSACTION_READ_COMMITTED, true, "select pkey from servers");
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private static Map<Integer,Boolean> aoServers=new HashMap<Integer,Boolean>();
    public static boolean isAOServer(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, ServerHandler.class, "isAOServer(MasterDatabaseConnection,int)", null);
        try {
            Integer I=Integer.valueOf(pkey);
            synchronized(aoServers) {
                if(aoServers.containsKey(I)) return aoServers.get(I).booleanValue();
                boolean isAOServer=conn.executeBooleanQuery(
                    "select (select server from ao_servers where server=?) is not null",
                    pkey
                );
                aoServers.put(I, isAOServer);
                return isAOServer;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void invalidateTable(SchemaTable.TableID tableID) {
        Profiler.startProfile(Profiler.FAST, ServerHandler.class, "invalidateTable(SchemaTable.TableID)", null);
        try {
            if(tableID==SchemaTable.TableID.AO_SERVERS) {
                synchronized(aoServers) {
                    aoServers.clear();
                }
            } else if(tableID==SchemaTable.TableID.BUSINESS_SERVERS) {
                synchronized(ServerHandler.class) {
                    usernameServers=null;
                }
            } else if(tableID==SchemaTable.TableID.MASTER_SERVERS) {
                synchronized(ServerHandler.class) {
                    usernameServers=null;
                }
            } else if(tableID==SchemaTable.TableID.SERVERS) {
                synchronized(failoverServers) {
                    failoverServers.clear();
                }
                synchronized(farmsForServers) {
                    farmsForServers.clear();
                }
                synchronized(hostnamesForAOServers) {
                    hostnamesForAOServers.clear();
                }
                synchronized(serversForAOServers) {
                    serversForAOServers.clear();
                }
            } else if(tableID==SchemaTable.TableID.SERVER_FARMS) {
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private static final Object invalidateSyncLock=new Object();

    /**
     * HashMap(server)->HashMap(Long(id))->RequestSource
     */
    private static final Map<Integer,Map<Long,RequestSource>> invalidateSyncEntries=new HashMap<Integer,Map<Long,RequestSource>>();

    /**
     * HashMap(Server)->Long(lastID)
     */
    private static final Map<Integer,Long> lastIDs=new HashMap<Integer,Long>();

    public static Long addInvalidateSyncEntry(int server, RequestSource source) {
        Profiler.startProfile(Profiler.FAST, ServerHandler.class, "addInvalidateSyncEntry(int,RequestSource)", null);
        try {
            Integer S=Integer.valueOf(server);
            synchronized(invalidateSyncLock) {
                long id;
                Long L=lastIDs.get(S);
                if(L==null) id=0;
                else id=L.longValue();
                Long idLong=Long.valueOf(id);
                lastIDs.put(S, idLong);

                Map<Long,RequestSource> ids=invalidateSyncEntries.get(S);
                if(ids==null) invalidateSyncEntries.put(S, ids=new HashMap<Long,RequestSource>());
                ids.put(idLong, source);

                return idLong;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    public static void removeInvalidateSyncEntry(int server, Long id) {
        Profiler.startProfile(Profiler.FAST, ServerHandler.class, "removeInvalidateSyncEntry(int,Long)", null);
        try {
            Integer S=Integer.valueOf(server);
            synchronized(invalidateSyncLock) {
                Map<Long,RequestSource> ids=invalidateSyncEntries.get(S);
                if(ids!=null) ids.remove(id);
                invalidateSyncLock.notify();
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void waitForInvalidates(int server) {
        Profiler.startProfile(Profiler.UNKNOWN, ServerHandler.class, "waitForInvalidates(int)", null);
        try {
            Integer S=Integer.valueOf(server);
            synchronized(invalidateSyncLock) {
                Long L=lastIDs.get(S);
                if(L!=null) {
                    long lastID=L.longValue();
                    Map<Long,RequestSource> ids=invalidateSyncEntries.get(S);
                    if(ids!=null) {
                        // Wait until the most recent ID and all previous IDs have been completed, but do
                        // not wait for more than 60 seconds total to prevent locked-up daemons from
                        // locking up everything.
                        long startTime=System.currentTimeMillis();
                        while(true) {
                            long maxWait=startTime+60000-System.currentTimeMillis();
                            if(maxWait>0 && maxWait<=60000) {
                                LongList closedIDs=null;
                                Iterator<Long> I=ids.keySet().iterator();
                                boolean foundOlder=false;
                                while(I.hasNext()) {
                                    Long idLong=I.next();
                                    RequestSource source=(RequestSource)ids.get(idLong);
                                    if(source.isClosed()) {
                                        if(closedIDs==null) closedIDs=new LongArrayList();
                                        closedIDs.add(idLong);
                                    } else {
                                        long id=idLong.longValue();
                                        if(id<=lastID) {
                                            foundOlder=true;
                                            break;
                                        }
                                    }
                                }
                                if(closedIDs!=null) {
                                    int size=closedIDs.size();
                                    for(int c=0;c<size;c++) ids.remove(closedIDs.get(c));
                                }
                                if(foundOlder) {
                                    try {
                                        invalidateSyncLock.wait(maxWait);
                                    } catch(InterruptedException err) {
                                        MasterServer.reportWarning(err, null);
                                    }
                                } else {
                                    invalidateSyncLock.notify();
                                    return;
                                }
                            } else {
                                System.err.println("waitForInvalidates has taken more than 60 seconds, returning even though the invalidates have not completed synchronization: "+server);
                                Thread.dumpStack();
                                invalidateSyncLock.notify();
                                return;
                            }
                        }
                    }
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    /**
     * Gets the package that owns the server.
     */
    public static int getPackageForServer(MasterDatabaseConnection conn, int server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, ServerHandler.class, "getPackageForServer(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select package from servers where pkey=?", server);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
   
    /**
     * Gets the per-package unique name of the server.
     */
    public static String getNameForServer(MasterDatabaseConnection conn, int server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, ServerHandler.class, "getPackageForServer(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select name from servers where pkey=?", server);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}