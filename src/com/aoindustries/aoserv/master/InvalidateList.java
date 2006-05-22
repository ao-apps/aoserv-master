package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 2200 Dogwood Ct N, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.profiler.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * In the request lifecycle, table invalidations occur after the database connection has been committed
 * and released.  This ensures that all data is available for the processes that react to the table
 * updates.  For effeciency, each server and accounting code will only be notified once per table per
 * request.
 *
 * @author  AO Industries, Inc.
 */
final public class InvalidateList {

    private static String[] tableNames=new String[SchemaTable.NUM_TABLES];

    /**
     * Indicates that all servers or businesses should receive the invalidate signal.
     */
    private static final String ALL="*** ALL ***";
    public static final Collection<String> allBusinesses=Collections.unmodifiableCollection(new ArrayList<String>());
    public static final Collection<String> allServers=Collections.unmodifiableCollection(new ArrayList<String>());

    private Map<Integer,List<String>> serverLists=new HashMap<Integer,List<String>>();
    private Map<Integer,List<String>> businessLists=new HashMap<Integer,List<String>>();

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
        int tableID,
        String business,
        int server,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "addTable(MasterDatabaseConnection,int,String,int,boolean)", null);
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
        int tableID,
        String business,
        String server,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "addTable(MasterDatabaseConnection,int,String,String,boolean)", null);
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
        int tableID,
        Collection<String> businesses,
        int server,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "addTable(MasterDatabaseConnection,int,Collection<String>,int,boolean)", null);
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
        int tableID,
        String business,
        IntCollection servers,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "addTable(MasterDatabaseConnection,int,String,IntCollection,boolean)", null);
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
        int tableID,
        Collection<String> businesses,
        IntCollection servers,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "addTable(MasterDatabaseConnection,int,Collection<String>,IntCollection,boolean)", null);
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
        int tableID,
        String business,
        Collection<String> servers,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "addTable(MasterDatabaseConnection,int,String,Collection<String>,boolean)", null);
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
        int tableID,
        Collection<String> businesses,
        Collection<String> servers,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "addTable(MasterDatabaseConnection,int,Collection<String>,Collection<String>,boolean)", null);
        try {
            // Warn about any mismatches
            if(businesses==allServers) MasterServer.reportWarning(new IllegalArgumentException("businesses==allServers"), null);
            if(servers==allBusinesses) MasterServer.reportWarning(new IllegalArgumentException("servers==allBusinesses"), null);

            if(tableNames[tableID]==null) tableNames[tableID]=TableHandler.getTableName(conn, tableID);
            Integer tableIDInteger = Integer.valueOf(tableID);

            // Add to the business lists
            {
                List<String> SV=businessLists.get(tableIDInteger);
                if(SV==null) {
                    SV=new SortedArrayList<String>();
                    businessLists.put(tableIDInteger, SV);
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
                List<String> SV=serverLists.get(tableIDInteger);
                if(SV==null) {
                    SV=new SortedArrayList<String>();
                    serverLists.put(tableIDInteger, SV);
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
                    case SchemaTable.AO_SERVERS :
                        addTable(conn, SchemaTable.INTERBASE_SERVER_USERS, businesses, servers, true);
                        addTable(conn, SchemaTable.LINUX_SERVER_ACCOUNTS, businesses, servers, true);
                        addTable(conn, SchemaTable.LINUX_SERVER_GROUPS, businesses, servers, true);
                        addTable(conn, SchemaTable.MYSQL_SERVERS, businesses, servers, true);
                        addTable(conn, SchemaTable.NET_DEVICES, businesses, servers, true);
                        addTable(conn, SchemaTable.POSTGRES_SERVERS, businesses, servers, true);
                        break;
                    case SchemaTable.BUSINESS_SERVERS :
                        addTable(conn, SchemaTable.SERVERS, businesses, servers, true);
                        break;
                    case SchemaTable.BUSINESSES :
                        addTable(conn, SchemaTable.BUSINESS_PROFILES, businesses, servers, true);
                        break;
                    case SchemaTable.EMAIL_DOMAINS :
                        addTable(conn, SchemaTable.EMAIL_ADDRESSES, businesses, servers, true);
                        addTable(conn, SchemaTable.MAJORDOMO_SERVERS, businesses, servers, true);
                        break;
                    case SchemaTable.FAILOVER_FILE_REPLICATIONS :
                        addTable(conn, SchemaTable.SERVERS, businesses, servers, true);
                        addTable(conn, SchemaTable.NET_DEVICES, businesses, servers, true);
                        addTable(conn, SchemaTable.IP_ADDRESSES, businesses, servers, true);
                        addTable(conn, SchemaTable.NET_BINDS, businesses, servers, true);
                        break;
                    case SchemaTable.FILE_BACKUPS :
                        addTable(conn, SchemaTable.BACKUP_DATA, businesses, servers, true);
                        break;
                    case SchemaTable.HTTPD_BINDS :
                        addTable(conn, SchemaTable.IP_ADDRESSES, businesses, servers, true);
                        addTable(conn, SchemaTable.NET_BINDS, businesses, servers, true);
                        break;
                    case SchemaTable.HTTPD_SITE_BINDS :
                        addTable(conn, SchemaTable.HTTPD_BINDS, businesses, servers, true);
                        break;
                    case SchemaTable.INTERBASE_BACKUPS :
                        addTable(conn, SchemaTable.BACKUP_DATA, businesses, servers, true);
                        break;
                    case SchemaTable.INTERBASE_SERVER_USERS :
                        addTable(conn, SchemaTable.INTERBASE_USERS, businesses, servers, true);
                        break;
                    case SchemaTable.LINUX_ACCOUNTS :
                        addTable(conn, SchemaTable.FTP_GUEST_USERS, businesses, servers, true);
                        addTable(conn, SchemaTable.USERNAMES, businesses, servers, true);
                        break;
                    case SchemaTable.LINUX_SERVER_ACCOUNTS :
                        addTable(conn, SchemaTable.LINUX_ACCOUNTS, businesses, servers, true);
                        addTable(conn, SchemaTable.LINUX_GROUP_ACCOUNTS, businesses, servers, true);
                        break;
                    case SchemaTable.LINUX_SERVER_GROUPS :
                        addTable(conn, SchemaTable.EMAIL_LISTS, businesses, servers, true);
                        addTable(conn, SchemaTable.LINUX_GROUPS, businesses, servers, true);
                        addTable(conn, SchemaTable.LINUX_GROUP_ACCOUNTS, businesses, servers, true);
                        break;
                    case SchemaTable.MAJORDOMO_SERVERS :
                        addTable(conn, SchemaTable.MAJORDOMO_LISTS, businesses, servers, true);
                        break;
                    case SchemaTable.MYSQL_BACKUPS :
                        addTable(conn, SchemaTable.BACKUP_DATA, businesses, servers, true);
                        break;
                    case SchemaTable.MYSQL_SERVER_USERS :
                        addTable(conn, SchemaTable.MYSQL_USERS, businesses, servers, true);
                        break;
                    case SchemaTable.MYSQL_SERVERS :
                        addTable(conn, SchemaTable.NET_BINDS, businesses, servers, true);
                        addTable(conn, SchemaTable.MYSQL_DATABASES, businesses, servers, true);
                        addTable(conn, SchemaTable.MYSQL_SERVER_USERS, businesses, servers, true);
                        break;
                    case SchemaTable.NET_DEVICES :
                        addTable(conn, SchemaTable.IP_ADDRESSES, businesses, servers, true);
                        break;
                    case SchemaTable.PACKAGE_DEFINITIONS :
                        addTable(conn, SchemaTable.PACKAGE_DEFINITION_LIMITS, businesses, servers, true);
                        break;
                    case SchemaTable.PACKAGES :
                        addTable(conn, SchemaTable.PACKAGE_DEFINITIONS, businesses, servers, true);
                        break;
                    case SchemaTable.POSTGRES_BACKUPS :
                        addTable(conn, SchemaTable.BACKUP_DATA, businesses, servers, true);
                        break;
                    case SchemaTable.POSTGRES_SERVER_USERS :
                        addTable(conn, SchemaTable.POSTGRES_USERS, businesses, servers, true);
                        break;
                    case SchemaTable.POSTGRES_SERVERS :
                        addTable(conn, SchemaTable.NET_BINDS, businesses, servers, true);
                        addTable(conn, SchemaTable.POSTGRES_DATABASES, businesses, servers, true);
                        addTable(conn, SchemaTable.POSTGRES_SERVER_USERS, businesses, servers, true);
                        break;
                    case SchemaTable.SERVERS :
                        addTable(conn, SchemaTable.AO_SERVERS, businesses, servers, true);
                        break;
                    case SchemaTable.USERNAMES :
                        addTable(conn, SchemaTable.BUSINESS_ADMINISTRATORS, businesses, servers, true);
                        break;
                }
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public List<String> getAffectedBusinesses(int tableID) {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "getAffectedBusinesses(int)", null);
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

    public List<String> getAffectedServers(int tableID) {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "getAffectedServers(int)", null);
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
            for(int c=0;c<SchemaTable.NUM_TABLES;c++) {
                if(serverLists.containsKey(c) || businessLists.containsKey(c)) {
                    BackupDatabaseSynchronizer.invalidateTable(c);
                    BackupHandler.invalidateTable(c);
                    BusinessHandler.invalidateTable(c);
                    CvsHandler.invalidateTable(c);
                    DaemonHandler.invalidateTable(c);
                    DNSHandler.invalidateTable(c);
                    EmailHandler.invalidateTable(c);
                    HttpdHandler.invalidateTable(c);
                    InterBaseHandler.invalidateTable(c);
                    LinuxAccountHandler.invalidateTable(c);
                    MasterServer.invalidateTable(c);
                    MySQLHandler.invalidateTable(c);
                    PackageHandler.invalidateTable(c);
                    PostgresHandler.invalidateTable(c);
                    ServerHandler.invalidateTable(c);
                    TableHandler.invalidateTable(c);
                    UsernameHandler.invalidateTable(c);
                }
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public boolean isInvalid(int tableID) {
        Profiler.startProfile(Profiler.FAST, InvalidateList.class, "isInvalid(int)", null);
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
