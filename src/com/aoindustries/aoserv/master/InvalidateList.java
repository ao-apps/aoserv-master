package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.profiler.Profiler;
import com.aoindustries.util.IntCollection;
import com.aoindustries.util.SortedArrayList;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * In the request lifecycle, table invalidations occur after the database connection has been committed
 * and released.  This ensures that all data is available for the processes that react to the table
 * updates.  For effeciency, each server and accounting code will only be notified once per table per
 * request.
 *
 * @author  AO Industries, Inc.
 */
final public class InvalidateList {

    final private static String[] tableNames=new String[SchemaTable.TableID.values().length];

    /**
     * Indicates that all servers or businesses should receive the invalidate signal.
     */
    private static final String ALL="*** ALL ***";
    public static final Collection<String> allBusinesses=Collections.unmodifiableCollection(new ArrayList<String>());
    public static final Collection<String> allServers=Collections.unmodifiableCollection(new ArrayList<String>());

    private Map<SchemaTable.TableID,List<String>> serverLists=new EnumMap<SchemaTable.TableID,List<String>>(SchemaTable.TableID.class);
    private Map<SchemaTable.TableID,List<String>> businessLists=new EnumMap<SchemaTable.TableID,List<String>>(SchemaTable.TableID.class);

    public void clear() {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "clear()", null);
        try {
            // Clear the servers
            Iterator<List<String>> sLists = serverLists.values().iterator();
            while(sLists.hasNext()) sLists.next().clear();
            Iterator<List<String>> bLists = businessLists.values().iterator();
            while(bLists.hasNext()) bLists.next().clear();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public void addTable(
        MasterDatabaseConnection conn,
        SchemaTable.TableID tableID,
        String business,
        int server,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "addTable(MasterDatabaseConnection,SchemaTable.TableID,String,int,boolean)", null);
        try {
            addTable(
                conn,
                tableID,
                getCollection(business),
                getServerCollection(conn, server),
                recurse
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public void addTable(
        MasterDatabaseConnection conn,
        SchemaTable.TableID tableID,
        String business,
        String server,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "addTable(MasterDatabaseConnection,SchemaTable.TableID,String,String,boolean)", null);
        try {
            addTable(
                conn,
                tableID,
                getCollection(business),
                getCollection(server),
                recurse
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public void addTable(
        MasterDatabaseConnection conn,
        SchemaTable.TableID tableID,
        Collection<String> businesses,
        int server,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "addTable(MasterDatabaseConnection,SchemaTable.TableID,Collection<String>,int,boolean)", null);
        try {
            addTable(
                conn,
                tableID,
                businesses,
                getServerCollection(conn, server),
                recurse
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public void addTable(
        MasterDatabaseConnection conn,
        SchemaTable.TableID tableID,
        String business,
        IntCollection servers,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "addTable(MasterDatabaseConnection,SchemaTable.TableID,String,IntCollection,boolean)", null);
        try {
            addTable(
                conn,
                tableID,
                getCollection(business),
                getServerCollection(conn, servers),
                recurse
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public void addTable(
        MasterDatabaseConnection conn,
        SchemaTable.TableID tableID,
        Collection<String> businesses,
        IntCollection servers,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "addTable(MasterDatabaseConnection,SchemaTable.TableID,Collection<String>,IntCollection,boolean)", null);
        try {
            addTable(
                conn,
                tableID,
                businesses,
                getServerCollection(conn, servers),
                recurse
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public void addTable(
        MasterDatabaseConnection conn,
        SchemaTable.TableID tableID,
        String business,
        Collection<String> servers,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "addTable(MasterDatabaseConnection,SchemaTable.TableID,String,Collection<String>,boolean)", null);
        try {
            addTable(
                conn,
                tableID,
                getCollection(business),
                servers,
                recurse
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public void addTable(
        MasterDatabaseConnection conn,
        SchemaTable.TableID tableID,
        Collection<String> businesses,
        Collection<String> servers,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "addTable(MasterDatabaseConnection,SchemaTable.TableID,Collection<String>,Collection<String>,boolean)", null);
        try {
            // Warn about any mismatches
            if(businesses==allServers) MasterServer.reportWarning(new IllegalArgumentException("businesses==allServers"), null);
            if(servers==allBusinesses) MasterServer.reportWarning(new IllegalArgumentException("servers==allBusinesses"), null);

            if(tableNames[tableID.ordinal()]==null) tableNames[tableID.ordinal()]=TableHandler.getTableName(conn, tableID);

            // Add to the business lists
            {
                List<String> SV=businessLists.get(tableID);
                if(SV==null) {
                    SV=new SortedArrayList<String>();
                    businessLists.put(tableID, SV);
                }
                if(!SV.contains(ALL)) {
                    if(businesses==null || businesses==allBusinesses) {
                        SV.clear();
                        SV.add(ALL);
                    } else {
                        for(String accounting : businesses) {
                            if(accounting==null) MasterServer.reportWarning(new RuntimeException("Warning: accounting is null"), null);
                            else if(!SV.contains(accounting)) SV.add(accounting);
                        }
                    }
                }
            }

            // Add to the server lists
            {
                List<String> SV=serverLists.get(tableID);
                if(SV==null) {
                    SV=new SortedArrayList<String>();
                    serverLists.put(tableID, SV);
                }
                if(!SV.contains(ALL)) {
                    if(servers==null || servers==allServers) {
                        SV.clear();
                        SV.add(ALL);
                    } else {
                        for(String hostname : servers) {
                            if(hostname==null) MasterServer.reportWarning(new RuntimeException("Warning: hostname is null"), null);
                            else if(!SV.contains(hostname)) SV.add(hostname);
                        }
                    }
                }
            }

            // Recursively invalidate those tables who's filters might have been effected
            if(recurse) {
                switch(tableID) {
                    case AO_SERVERS :
                        addTable(conn, SchemaTable.TableID.INTERBASE_SERVER_USERS, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.LINUX_SERVER_ACCOUNTS, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.LINUX_SERVER_GROUPS, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.MYSQL_SERVERS, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.NET_DEVICES, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.POSTGRES_SERVERS, businesses, servers, true);
                        break;
                    case BUSINESS_SERVERS :
                        addTable(conn, SchemaTable.TableID.SERVERS, businesses, servers, true);
                        break;
                    case BUSINESSES :
                        addTable(conn, SchemaTable.TableID.BUSINESS_PROFILES, businesses, servers, true);
                        break;
                    case EMAIL_DOMAINS :
                        addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.MAJORDOMO_SERVERS, businesses, servers, true);
                        break;
                    case FAILOVER_FILE_REPLICATIONS :
                        addTable(conn, SchemaTable.TableID.SERVERS, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.NET_DEVICES, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.IP_ADDRESSES, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.NET_BINDS, businesses, servers, true);
                        break;
                    case FILE_BACKUPS :
                        addTable(conn, SchemaTable.TableID.BACKUP_DATA, businesses, servers, true);
                        break;
                    case HTTPD_BINDS :
                        addTable(conn, SchemaTable.TableID.IP_ADDRESSES, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.NET_BINDS, businesses, servers, true);
                        break;
                    case HTTPD_SITE_BINDS :
                        addTable(conn, SchemaTable.TableID.HTTPD_BINDS, businesses, servers, true);
                        break;
                    case INTERBASE_BACKUPS :
                        addTable(conn, SchemaTable.TableID.BACKUP_DATA, businesses, servers, true);
                        break;
                    case INTERBASE_SERVER_USERS :
                        addTable(conn, SchemaTable.TableID.INTERBASE_USERS, businesses, servers, true);
                        break;
                    case LINUX_ACCOUNTS :
                        addTable(conn, SchemaTable.TableID.FTP_GUEST_USERS, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.USERNAMES, businesses, servers, true);
                        break;
                    case LINUX_SERVER_ACCOUNTS :
                        addTable(conn, SchemaTable.TableID.LINUX_ACCOUNTS, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.LINUX_GROUP_ACCOUNTS, businesses, servers, true);
                        break;
                    case LINUX_SERVER_GROUPS :
                        addTable(conn, SchemaTable.TableID.EMAIL_LISTS, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.LINUX_GROUPS, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.LINUX_GROUP_ACCOUNTS, businesses, servers, true);
                        break;
                    case MAJORDOMO_SERVERS :
                        addTable(conn, SchemaTable.TableID.MAJORDOMO_LISTS, businesses, servers, true);
                        break;
                    case MYSQL_BACKUPS :
                        addTable(conn, SchemaTable.TableID.BACKUP_DATA, businesses, servers, true);
                        break;
                    case MYSQL_SERVER_USERS :
                        addTable(conn, SchemaTable.TableID.MYSQL_USERS, businesses, servers, true);
                        break;
                    case MYSQL_SERVERS :
                        addTable(conn, SchemaTable.TableID.NET_BINDS, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.MYSQL_DATABASES, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.MYSQL_SERVER_USERS, businesses, servers, true);
                        break;
                    case NET_DEVICES :
                        addTable(conn, SchemaTable.TableID.IP_ADDRESSES, businesses, servers, true);
                        break;
                    case PACKAGE_DEFINITIONS :
                        addTable(conn, SchemaTable.TableID.PACKAGE_DEFINITION_LIMITS, businesses, servers, true);
                        break;
                    case PACKAGES :
                        addTable(conn, SchemaTable.TableID.PACKAGE_DEFINITIONS, businesses, servers, true);
                        break;
                    case POSTGRES_BACKUPS :
                        addTable(conn, SchemaTable.TableID.BACKUP_DATA, businesses, servers, true);
                        break;
                    case POSTGRES_SERVER_USERS :
                        addTable(conn, SchemaTable.TableID.POSTGRES_USERS, businesses, servers, true);
                        break;
                    case POSTGRES_SERVERS :
                        addTable(conn, SchemaTable.TableID.NET_BINDS, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.POSTGRES_DATABASES, businesses, servers, true);
                        addTable(conn, SchemaTable.TableID.POSTGRES_SERVER_USERS, businesses, servers, true);
                        break;
                    case SERVERS :
                        addTable(conn, SchemaTable.TableID.AO_SERVERS, businesses, servers, true);
                        break;
                    case USERNAMES :
                        addTable(conn, SchemaTable.TableID.BUSINESS_ADMINISTRATORS, businesses, servers, true);
                        break;
                }
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public List<String> getAffectedBusinesses(SchemaTable.TableID tableID) {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "getAffectedBusinesses(SchemaTable.TableID)", null);
        try {
            List<String> SV=businessLists.get(tableID);
            if(SV!=null || serverLists.containsKey(tableID)) {
                if(SV==null) return new SortedArrayList<String>();
                if(SV.size()==0) return SV;
                if(SV.contains(ALL)) return new SortedArrayList<String>();
                return SV;
            } else return null;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public List<String> getAffectedServers(SchemaTable.TableID tableID) {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "getAffectedServers(SchemaTable.TableID)", null);
        try {
            List<String> SV=serverLists.get(tableID);
            if(SV!=null || businessLists.containsKey(tableID)) {
                if(SV==null) return new SortedArrayList<String>();
                if(SV.size()==0) return SV;
                if(SV.contains(ALL)) return new SortedArrayList<String>();
                return SV;
            } else return null;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public void invalidateMasterCaches() {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "invalidateMasterCaches()", null);
        try {
            for(SchemaTable.TableID tableID : SchemaTable.TableID.values()) {
                if(serverLists.containsKey(tableID) || businessLists.containsKey(tableID)) {
                    BackupDatabaseSynchronizer.invalidateTable(tableID);
                    BackupHandler.invalidateTable(tableID);
                    BusinessHandler.invalidateTable(tableID);
                    CvsHandler.invalidateTable(tableID);
                    DaemonHandler.invalidateTable(tableID);
                    DNSHandler.invalidateTable(tableID);
                    EmailHandler.invalidateTable(tableID);
                    HttpdHandler.invalidateTable(tableID);
                    InterBaseHandler.invalidateTable(tableID);
                    LinuxAccountHandler.invalidateTable(tableID);
                    MasterServer.invalidateTable(tableID);
                    MySQLHandler.invalidateTable(tableID);
                    PackageHandler.invalidateTable(tableID);
                    PostgresHandler.invalidateTable(tableID);
                    ServerHandler.invalidateTable(tableID);
                    TableHandler.invalidateTable(tableID);
                    UsernameHandler.invalidateTable(tableID);
                }
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public boolean isInvalid(SchemaTable.TableID tableID) {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "isInvalid(SchemaTable.TableID)", null);
        try {
            return serverLists.containsKey(tableID) || businessLists.containsKey(tableID);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    public static Collection<String> getCollection(String ... params) {
        final int PROFILER_LEVEL=Profiler.FAST;
        Profiler.startProfile(PROFILER_LEVEL, InvalidateList.class, "getCollection(String...)", null);
        try {
            if(params.length==0) return Collections.emptyList();
            Collection<String> coll = new ArrayList<String>(params.length);
            Collections.addAll(coll, params);
            return coll;
        } finally {
            Profiler.endProfile(PROFILER_LEVEL);
        }
    }

    public static Collection<String> getServerCollection(MasterDatabaseConnection conn, int ... serverPKeys) throws IOException, SQLException {
        final int PROFILER_LEVEL=Profiler.FAST;
        Profiler.startProfile(PROFILER_LEVEL, InvalidateList.class, "getServerCollection(MasterDatabaseConnection,int...)", null);
        try {
            if(serverPKeys.length==0) return Collections.emptyList();
            Collection<String> coll = new ArrayList<String>(serverPKeys.length);
            for(int pkey : serverPKeys) coll.add(ServerHandler.getHostnameForServer(conn, pkey));
            return coll;
        } finally {
            Profiler.endProfile(PROFILER_LEVEL);
        }
    }

    public static Collection<String> getServerCollection(MasterDatabaseConnection conn, IntCollection serverPKeys) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InvalidateList.class, "getServerCollection(MasterDatabaseConnection,IntCollection)", null);
        try {
            if(serverPKeys.isEmpty()) return Collections.emptyList();
            Collection<String> coll = new ArrayList<String>(serverPKeys.size());
            for(Integer pkey : serverPKeys) coll.add(ServerHandler.getHostnameForServer(conn, pkey));
            return coll;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}
