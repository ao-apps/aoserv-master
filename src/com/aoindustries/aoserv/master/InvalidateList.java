/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.dbc.DatabaseAccess;
import com.aoindustries.util.IntArrayList;
import com.aoindustries.util.IntCollection;
import com.aoindustries.util.IntList;
import com.aoindustries.util.SortedArrayList;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * In the request lifecycle, table invalidations occur after the database connection has been committed
 * and released.  This ensures that all data is available for the processes that react to the table
 * updates.  For efficiency, each server and accounting code will only be notified once per table per
 * request.
 *
 * @author  AO Industries, Inc.
 */
final public class InvalidateList {

    /**
     * The invalidate list is used as part of the error logging, so it is not
     * logged to the ticket system.
     */
    private static final Logger logger = Logger.getLogger(InvalidateList.class.getName());

    /** Copy once to avoid repeated copies. */
    final private static SchemaTable.TableID[] tableIDs = SchemaTable.TableID.values();
    // TODO: Unused 2018-11-18: final private static int numTables = tableIDs.length;

    // TODO: Unused 2018-11-18: final private static String[] tableNames=new String[numTables];

    /**
     * Indicates that all servers or account.Account should receive the invalidate signal.
     */
    public static final List<AccountingCode> allBusinesses=Collections.unmodifiableList(new ArrayList<AccountingCode>());
    public static final IntList allServers=new IntArrayList();

    private final Map<SchemaTable.TableID,List<Integer>> serverLists=new EnumMap<>(SchemaTable.TableID.class);
    private final Map<SchemaTable.TableID,List<AccountingCode>> businessLists=new EnumMap<>(SchemaTable.TableID.class);

    public void addTable(
        DatabaseAccess conn,
        SchemaTable.TableID tableID,
        AccountingCode business,
        int server,
        boolean recurse
    ) throws IOException, SQLException {
        addTable(
            conn,
            tableID,
            getCollection(business),
            getServerCollection(server),
            recurse
        );
    }

    public void addTable(
        DatabaseAccess conn,
        SchemaTable.TableID tableID,
        Collection<AccountingCode> businesses,
        int server,
        boolean recurse
    ) throws IOException, SQLException {
        addTable(
            conn,
            tableID,
            businesses,
            getServerCollection(server),
            recurse
        );
    }

    public void addTable(
        DatabaseAccess conn,
        SchemaTable.TableID tableID,
        AccountingCode business,
        IntCollection servers,
        boolean recurse
    ) throws IOException, SQLException {
        addTable(
            conn,
            tableID,
            getCollection(business),
            servers,
            recurse
        );
    }

    public void addTable(
        DatabaseAccess conn,
        SchemaTable.TableID tableID,
        Collection<AccountingCode> businesses,
        IntCollection servers,
        boolean recurse
    ) throws IOException, SQLException {
        // TODO: Unused 2018-11-18: if(tableNames[tableID.ordinal()]==null) tableNames[tableID.ordinal()]=TableHandler.getTableName(conn, tableID);

        // Add to the business lists
        {
            if(businesses==null || businesses==allBusinesses) businessLists.put(tableID, allBusinesses);
            else {
                List<AccountingCode> SV=businessLists.get(tableID);
                if(SV==null) businessLists.put(tableID, SV=new SortedArrayList<>());
                for(AccountingCode accounting : businesses) {
                    if(accounting==null) logger.log(Level.WARNING, null, new RuntimeException("Warning: accounting is null"));
                    else if(!SV.contains(accounting)) SV.add(accounting);
                }
            }
        }

        // Add to the server lists
        {
            if(servers==null || servers==allServers) serverLists.put(tableID, allServers);
            else {
                List<Integer> SV=serverLists.get(tableID);
                if(SV==null) serverLists.put(tableID, SV=new SortedArrayList<>());
                for(Integer pkey : servers) {
                    if(pkey==null) logger.log(Level.WARNING, null, new RuntimeException("Warning: pkey is null"));
                    else if(!SV.contains(pkey)) SV.add(pkey);
                }
            }
        }

        // Recursively invalidate those tables who's filters might have been effected
        if(recurse) {
            switch(tableID) {
                case AO_SERVERS :
                    addTable(conn, SchemaTable.TableID.FIREWALLD_ZONES, businesses, servers, true);
                    addTable(conn, SchemaTable.TableID.LINUX_SERVER_ACCOUNTS, businesses, servers, true);
                    addTable(conn, SchemaTable.TableID.LINUX_SERVER_GROUPS, businesses, servers, true);
                    addTable(conn, SchemaTable.TableID.MYSQL_SERVERS, businesses, servers, true);
                    addTable(conn, SchemaTable.TableID.POSTGRES_SERVERS, businesses, servers, true);
                    break;
                case BUSINESS_SERVERS :
                    addTable(conn, SchemaTable.TableID.SERVERS, businesses, servers, true);
                    break;
                case BUSINESSES :
                    addTable(conn, SchemaTable.TableID.BUSINESS_PROFILES, businesses, servers, true);
                    break;
                case CYRUS_IMAPD_BINDS :
                    addTable(conn, SchemaTable.TableID.CYRUS_IMAPD_SERVERS, businesses, servers, false);
                    break;
                case CYRUS_IMAPD_SERVERS :
                    addTable(conn, SchemaTable.TableID.CYRUS_IMAPD_BINDS, businesses, servers, false);
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
                case IP_REPUTATION_LIMITER_SETS:
                    // Sets are only visible when used by at least one limiter in the same server farm
                    addTable(conn, SchemaTable.TableID.IP_REPUTATION_SETS,         businesses, servers, true);
                    addTable(conn, SchemaTable.TableID.IP_REPUTATION_SET_HOSTS,    businesses, servers, true);
                    addTable(conn, SchemaTable.TableID.IP_REPUTATION_SET_NETWORKS, businesses, servers, true);
                    break;
                case HTTPD_BINDS :
                    addTable(conn, SchemaTable.TableID.IP_ADDRESSES, businesses, servers, true);
                    addTable(conn, SchemaTable.TableID.NET_BINDS, businesses, servers, false);
                    break;
                case HTTPD_SITE_BINDS :
                    addTable(conn, SchemaTable.TableID.HTTPD_BINDS, businesses, servers, true);
                    addTable(conn, SchemaTable.TableID.HTTPD_SITE_BIND_HEADERS, businesses, servers, false);
                    addTable(conn, SchemaTable.TableID.HTTPD_SITE_BIND_REDIRECTS, businesses, servers, false);
                    break;
                case HTTPD_TOMCAT_SITES :
                    addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_SITE_JK_MOUNTS, businesses, servers, false);
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
                case MYSQL_SERVER_USERS :
                    addTable(conn, SchemaTable.TableID.MYSQL_USERS, businesses, servers, true);
                    break;
                case MYSQL_SERVERS :
                    addTable(conn, SchemaTable.TableID.NET_BINDS, businesses, servers, true);
                    addTable(conn, SchemaTable.TableID.MYSQL_DATABASES, businesses, servers, true);
                    addTable(conn, SchemaTable.TableID.MYSQL_SERVER_USERS, businesses, servers, true);
                    break;
                case NET_BINDS :
                    addTable(conn, SchemaTable.TableID.HTTPD_BINDS, businesses, servers, false);
                    addTable(conn, SchemaTable.TableID.NET_BIND_FIREWALLD_ZONES, businesses, servers, false);
                    break;
                case NET_BIND_FIREWALLD_ZONES :
					// Presence of "public" firewalld zone determines compatibility "open_firewall" for clients
					// version <= 1.80.2
                    addTable(conn, SchemaTable.TableID.NET_BINDS, businesses, servers, false);
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
                case POSTGRES_SERVER_USERS :
                    addTable(conn, SchemaTable.TableID.POSTGRES_USERS, businesses, servers, true);
                    break;
                case POSTGRES_SERVERS :
                    addTable(conn, SchemaTable.TableID.NET_BINDS, businesses, servers, true);
                    addTable(conn, SchemaTable.TableID.POSTGRES_DATABASES, businesses, servers, true);
                    addTable(conn, SchemaTable.TableID.POSTGRES_SERVER_USERS, businesses, servers, true);
                    break;
                case SENDMAIL_BINDS :
                    addTable(conn, SchemaTable.TableID.SENDMAIL_SERVERS, businesses, servers, false);
                    break;
                case SENDMAIL_SERVERS :
                    addTable(conn, SchemaTable.TableID.SENDMAIL_BINDS, businesses, servers, false);
                    break;
                case SERVERS :
                    addTable(conn, SchemaTable.TableID.AO_SERVERS, businesses, servers, true);
                    addTable(conn, SchemaTable.TableID.IP_ADDRESSES, businesses, servers, true);
                    addTable(conn, SchemaTable.TableID.NET_DEVICES, businesses, servers, true);
                    addTable(conn, SchemaTable.TableID.VIRTUAL_SERVERS, businesses, servers, true);
                    break;
                case SSL_CERTIFICATES :
                    addTable(conn, SchemaTable.TableID.SSL_CERTIFICATE_NAMES, businesses, servers, false);
                    addTable(conn, SchemaTable.TableID.SSL_CERTIFICATE_OTHER_USES, businesses, servers, false);
                    break;
                case USERNAMES :
                    addTable(conn, SchemaTable.TableID.BUSINESS_ADMINISTRATORS, businesses, servers, true);
                    break;
                case VIRTUAL_SERVERS :
                    addTable(conn, SchemaTable.TableID.VIRTUAL_DISKS, businesses, servers, true);
                    break;
            }
        }
    }

    public List<AccountingCode> getAffectedBusinesses(SchemaTable.TableID tableID) {
        List<AccountingCode> SV=businessLists.get(tableID);
        if(SV!=null || serverLists.containsKey(tableID)) {
            if(SV==null) return allBusinesses;
            return SV;
        } else return null;
    }

    public List<Integer> getAffectedServers(SchemaTable.TableID tableID) {
        List<Integer> SV=serverLists.get(tableID);
        if(SV!=null || businessLists.containsKey(tableID)) {
            if(SV==null) return allServers;
            return SV;
        } else return null;
    }

    public void invalidateMasterCaches() {
        for(SchemaTable.TableID tableID : tableIDs) {
            if(serverLists.containsKey(tableID) || businessLists.containsKey(tableID)) {
                BusinessHandler.invalidateTable(tableID);
                CvsHandler.invalidateTable(tableID);
                DaemonHandler.invalidateTable(tableID);
                DNSHandler.invalidateTable(tableID);
                EmailHandler.invalidateTable(tableID);
                HttpdHandler.invalidateTable(tableID);
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
    }

    public boolean isInvalid(SchemaTable.TableID tableID) {
        return serverLists.containsKey(tableID) || businessLists.containsKey(tableID);
    }
    
    public static Collection<AccountingCode> getCollection(AccountingCode ... params) {
        if(params.length==0) return Collections.emptyList();
        Collection<AccountingCode> coll = new ArrayList<>(params.length);
        Collections.addAll(coll, params);
        return coll;
    }

    public static IntCollection getServerCollection(int ... serverPKeys) throws IOException, SQLException {
        if(serverPKeys.length==0) return new IntArrayList(0);
        IntCollection coll = new IntArrayList(serverPKeys.length);
        for(int pkey : serverPKeys) coll.add(pkey);
        return coll;
    }
}
