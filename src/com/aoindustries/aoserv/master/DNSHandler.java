package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.profiler.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * The <code>DNSHandler</code> handles all the accesses to the DNS tables.
 *
 * @author  AO Industries, Inc.
 */
final public class DNSHandler {

    /**
     * Creates a new <code>DNSRecord</code>.
     */
    public static int addDNSRecord(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String zone,
        String domain,
        String type,
        int mx_priority,
        String destination,
        int ttl
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "addDNSRecord(MasterDatabaseConnection,RequestSource,InvalidateList,String,String,String,int,String,int)", null);
        try {
            // Must be allowed to access this zone
            checkAccessDNSZone(conn, source, "addDNSRecord", zone);

            // Must have appropriate MX priority
            boolean isMX=conn.executeBooleanQuery("select is_mx from dns_types where type=?", type);
            if(isMX) {
                if(mx_priority==DNSRecord.NO_MX_PRIORITY) throw new IllegalArgumentException("mx_priority required for type="+type);
                else if(mx_priority<=0) throw new SQLException("Invalid mx_priority: "+mx_priority);
            } else {
                if(mx_priority!=DNSRecord.NO_MX_PRIORITY) throw new SQLException("No mx_priority allowed for type="+type);
            }

            // Must have a valid destination type
            try {
                DNSType.checkDestination(
                    destination,
                    conn.executeBooleanQuery("select param_ip from dns_types where type=?", type)
                );
            } catch(IllegalArgumentException err) {
                throw new SQLException("Invalid destination: "+err.getMessage());
            }

            // Make all database changes in one big transaction
            int pkey=conn.executeIntQuery("select nextval('dns_records_pkey_seq')");

            // Add the entry
            PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into dns_records values(?,?,?,?,?,?,null,?)");
            try {
                pstmt.setInt(1, pkey);
                pstmt.setString(2, zone);
                pstmt.setString(3, domain);
                pstmt.setString(4, type);
                if(mx_priority==DNSRecord.NO_MX_PRIORITY) pstmt.setNull(5, Types.INTEGER);
                else pstmt.setInt(5, mx_priority);
                pstmt.setString(6, destination);
                if(ttl==-1) pstmt.setNull(7, Types.INTEGER);
                else pstmt.setInt(7, ttl);
                conn.incrementUpdateCount();
                pstmt.executeUpdate();
            } catch(SQLException err) {
                System.err.println("Error from query: "+pstmt.toString());
                throw err;
            } finally {
                pstmt.close();
            }
            invalidateList.addTable(conn, SchemaTable.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);

            // Update the serial of the zone
            updateDNSZoneSerial(conn, invalidateList, zone);

            // Notify all clients of the update
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Creates a new <code>DNSZone</code>.
     */
    public static void addDNSZone(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String packageName,
        String zone,
        String ip,
        int ttl
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "addDNSZone(MasterDatabaseConnection,RequestSource,InvalidateList,String,String,String,int)", null);
        try {
            // Must be allowed to access this package
            PackageHandler.checkAccessPackage(conn, source, "addDNSZone", packageName);
            if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Not allowed to add DNSZone to disabled Package: "+packageName);
            MasterServer.checkAccessHostname(conn, source, "addDNSZone", zone);
            // Check the zone format
            List<String> tlds=getDNSTLDs(conn);
            if(!DNSZoneTable.checkDNSZone(zone, tlds)) throw new SQLException("Invalid zone: "+zone);
            // Check the ip address format
            if(!IPAddress.isValidIPAddress(ip)) throw new SQLException("Invalid IP address: "+ip);

            // Must not be allocated in any way to another account
            MasterServer.checkAccessHostname(conn, source, "addDNSZone", zone);

            // Add the dns_zone entry
            PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into dns_zones values(?,?,?,?,?,?)");
            try {
                pstmt.setString(1, zone);
                pstmt.setString(2, zone);
                pstmt.setString(3, packageName);
                pstmt.setString(4, DNSZone.DEFAULT_HOSTMASTER);
                pstmt.setLong(5, DNSZone.getCurrentSerial());
                pstmt.setInt(6, ttl);
                conn.incrementUpdateCount();
                pstmt.executeUpdate();
            } finally {
                pstmt.close();
            }

            // Add the MX entry
            pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into dns_records(zone, domain, type, mx_priority, destination) values(?,?,?,?,?)");
            try {
                pstmt.setString(1, zone);
                pstmt.setString(2, "@");
                pstmt.setString(3, "MX");
                pstmt.setInt(4, 10);
                pstmt.setString(5, "mail");
                conn.incrementUpdateCount();
                pstmt.executeUpdate();
            } finally {
                pstmt.close();
            }

            pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into dns_records(zone, domain, type, destination) values(?,?,?,?)");
            try {
                // Add the ns1.aoindustries.com name server
                pstmt.setString(1, zone);
                pstmt.setString(2, "@");
                pstmt.setString(3, "NS");
                pstmt.setString(4, "ns1.aoindustries.com.");
                conn.incrementUpdateCount();
                pstmt.executeUpdate();

                // Add the ns2.aoindustries.com name server
                pstmt.setString(1, zone);
                pstmt.setString(2, "@");
                pstmt.setString(3, "NS");
                pstmt.setString(4, "ns2.aoindustries.com.");
                conn.incrementUpdateCount();
                pstmt.executeUpdate();

                // Add the ns3.aoindustries.com name server
                pstmt.setString(1, zone);
                pstmt.setString(2, "@");
                pstmt.setString(3, "NS");
                pstmt.setString(4, "ns3.aoindustries.com.");
                conn.incrementUpdateCount();
                pstmt.executeUpdate();

                // Add the ns4.aoindustries.com name server
                pstmt.setString(1, zone);
                pstmt.setString(2, "@");
                pstmt.setString(3, "NS");
                pstmt.setString(4, "ns4.aoindustries.com.");
                conn.incrementUpdateCount();
                pstmt.executeUpdate();

                // Add the domain IP
                pstmt.setString(1, zone);
                pstmt.setString(2, "@");
                pstmt.setString(3, "A");
                pstmt.setString(4, ip);
                conn.incrementUpdateCount();
                pstmt.executeUpdate();

                // Add the ftp IP
                pstmt.setString(1, zone);
                pstmt.setString(2, "ftp");
                pstmt.setString(3, "A");
                pstmt.setString(4, ip);
                conn.incrementUpdateCount();
                pstmt.executeUpdate();

                // Add the mail IP
                pstmt.setString(1, zone);
                pstmt.setString(2, "mail");
                pstmt.setString(3, "A");
                pstmt.setString(4, ip);
                conn.incrementUpdateCount();
                pstmt.executeUpdate();

                // Add the www IP
                pstmt.setString(1, zone);
                pstmt.setString(2, "www");
                pstmt.setString(3, "A");
                pstmt.setString(4, ip);
                conn.incrementUpdateCount();
                pstmt.executeUpdate();
            } finally {
                pstmt.close();
            }

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.DNS_ZONES, InvalidateList.allBusinesses, InvalidateList.allServers, false);
            invalidateList.addTable(conn, SchemaTable.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a <code>DNSRecord</code>.
     */
    public static void removeDNSRecord(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "removeDNSRecord(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            // Must be allowed to access this zone record
            checkAccessDNSRecord(conn, source, "removeDNSRecord", pkey);

            // Get the zone associated with the pkey
            String zone=getDNSZoneForDNSRecord(conn, pkey);

            // Remove the dns_records entry
            PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("delete from dns_records where pkey=?");
            try {
                pstmt.setInt(1, pkey);
                conn.incrementUpdateCount();
                pstmt.executeUpdate();
            } finally {
                pstmt.close();
            }
            invalidateList.addTable(conn, SchemaTable.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);

            // Update the serial of the zone
            updateDNSZoneSerial(conn, invalidateList, zone);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a <code>DNSZone</code>.
     */
    public static void removeDNSZone(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String zone
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, DNSHandler.class, "removeDNSZone(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            // Must be allowed to access this zone
            checkAccessDNSZone(conn, source, "removeDNSZone", zone);

            removeDNSZone(conn, invalidateList, zone);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Removes a <code>DNSZone</code>.
     */
    public static void removeDNSZone(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        String zone
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "removeDNSZone(MasterDatabaseConnection,InvalidateList,String)", null);
        try {
            // Remove the dns_records entries
            PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("delete from dns_records where zone=?");
            try {
                pstmt.setString(1, zone);
                conn.incrementUpdateCount();
                pstmt.executeUpdate();
            } finally {
                pstmt.close();
            }

            // Remove the dns_zones entry
            pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("delete from dns_zones where zone=?");
            try {
                pstmt.setString(1, zone);
                conn.incrementUpdateCount();
                pstmt.executeUpdate();
            } finally {
                pstmt.close();
            }

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
            invalidateList.addTable(conn, SchemaTable.DNS_ZONES, InvalidateList.allBusinesses, InvalidateList.allServers, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean addDNSRecord(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        String hostname,
        String ipAddress,
        List<String> tlds
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "addDNSRecord(MasterDatabaseConnection,InvalidateList,String,String,List<String>)", null);
        try {
            String tldPlus1 = DNSZoneTable.getHostTLD(hostname, tlds);
            boolean exists = conn.executeBooleanQuery(
                "select (select zone from dns_zones where zone=?) is not null",
                tldPlus1
            );
            if (exists) {
                String preTldPlus1 = hostname.substring(0, hostname.length()-tldPlus1.length());
                exists = conn.executeBooleanQuery(
                    "select (select pkey from dns_records where zone=? and type='A' and domain=?) is not null",
                    tldPlus1,
                    preTldPlus1
                );
                if (!exists) {
                    conn.executeUpdate(
                        "insert into dns_records (zone, domain, type, destination) values (?, ?, 'A', ?)",
                        tldPlus1,
                        preTldPlus1,
                        ipAddress
                    );
                    invalidateList.addTable(
                        conn,
                        SchemaTable.DNS_RECORDS,
                        getBusinessForDNSZone(conn, tldPlus1),
                        getDNSServers(conn),
                        false
                    );
                    updateDNSZoneSerial(conn, invalidateList, tldPlus1);
                    return true;
                }
            }
            return false;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessDNSRecord(MasterDatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, DNSHandler.class, "checkAccessDNSRecord(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            if(
                !isDNSAdmin(conn, source)
                && !PackageHandler.canAccessPackage(conn, source, getPackageForDNSRecord(conn, pkey))
            ) {
                String message=
                    "business_administrator.username="
                    +source.getUsername()
                    +" is not allowed to access dns_record: action='"
                    +action
                    +", pkey="
                    +pkey
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean canAccessDNSZone(MasterDatabaseConnection conn, RequestSource source, String zone) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, DNSHandler.class, "canAccessDNSZone(MasterDatabaseConnection,RequestSource,String)", null);
        try {
            return
                isDNSAdmin(conn, source)
                || PackageHandler.canAccessPackage(conn, source, getPackageForDNSZone(conn, zone))
            ;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void checkAccessDNSZone(MasterDatabaseConnection conn, RequestSource source, String action, String zone) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, DNSHandler.class, "checkAccessDNSZone(MasterDatabaseConnection,RequestSource,String,String)", null);
        try {
            if(!canAccessDNSZone(conn, source, zone)) {
                String message=
                    "business_administrator.username="
                    +source.getUsername()
                    +" is not allowed to access dns_zone: action='"
                    +action
                    +", zone='"
                    +zone
                    +'\''
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Admin access to named info is granted if either no server is restricted, or one of the
     * granted servers is a named machine.
     */
    public static boolean isDNSAdmin(
        MasterDatabaseConnection conn,
        RequestSource source
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, DNSHandler.class, "isDNSAdmin(MasterDatabaseConnection,RequestSource)", null);
        try {
            MasterUser mu=MasterServer.getMasterUser(conn, source.getUsername());
            return mu!=null && mu.isDNSAdmin();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getBusinessForDNSRecord(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "getBusinessForDNSRecord(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select pk.accounting from dns_records nr, dns_zones nz, packages pk where nr.zone=nz.zone and nz.package=pk.name and nr.pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForDNSZone(MasterDatabaseConnection conn, String zone) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "getBusinessForDNSZone(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeStringQuery("select pk.accounting from dns_zones nz, packages pk where nz.package=pk.name and nz.zone=?", zone);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static List<String> getDNSServers(MasterDatabaseConnection conn) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "getDNSServers(MasterDatabaseConnection)", null);
        try {
            return conn.executeStringListQuery("select se.hostname from ao_servers ao, servers se where ao.is_dns and ao.server=se.pkey");
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private static final Object dnstldLock=new Object();
    private static List<String> dnstldCache;
    public static List<String> getDNSTLDs(MasterDatabaseConnection conn) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, DNSHandler.class, "getDNSTLDs(MasterDatabaseConnection)", null);
        try {
            synchronized(dnstldLock) {
                if(dnstldCache==null) {
                    dnstldCache=conn.executeStringListQuery("select domain from dns_tlds");
                }
                return dnstldCache;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    public static String getDNSZoneForDNSRecord(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "getDNSZoneForDNSRecord(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select zone from dns_records where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isDNSZoneAvailable(MasterDatabaseConnection conn, String zone) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "isDNSZoneAvailable(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeBooleanQuery("select (select zone from dns_zones where zone=?) is null", zone);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getPackageForDNSRecord(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "getPackageForDNSRecord(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select nz.package from dns_records nr, dns_zones nz where nr.pkey=? and nr.zone=nz.zone", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getPackageForDNSZone(MasterDatabaseConnection conn, String zone) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "getPackageForDNSZone(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeStringQuery("select package from dns_zones where zone=?", zone);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void invalidateTable(int tableID) {
        Profiler.startProfile(Profiler.FAST, DNSHandler.class, "invalidateTable(int)", null);
        try {
            switch(tableID) {
                case SchemaTable.DNS_TLDS :
                    synchronized(dnstldLock) {
                        dnstldCache=null;
                    }
                    break;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void removeUnusedDNSRecord(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        String hostname,
        List<String> tlds
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "removeUnusedDNSRecord(MasterDatabaseConnection,InvalidateList,String,List<String>)", null);
        try {
            if(
                conn.executeBooleanQuery("select (select pkey from httpd_site_urls where hostname=? limit 1) is null", hostname)
            ) {
                String tldPlus1 = DNSZoneTable.getHostTLD(hostname, tlds);
                if(conn.executeBooleanQuery("select (select zone from dns_zones where zone=?) is not null", tldPlus1)) {
                    String preTldPlus1 = hostname.length()<=tldPlus1.length()?"@":hostname.substring(0, hostname.length()-tldPlus1.length());
                    int pkey=conn.executeIntQuery(
                        "select\n"
                        + "  coalesce(\n"
                        + "    (\n"
                        + "      select\n"
                        + "        pkey\n"
                        + "      from\n"
                        + "        dns_records\n"
                        + "      where\n"
                        + "        zone=?\n"
                        + "        and type='A'\n"
                        + "        and domain=?\n"
                        + "      limit 1\n"
                        + "    ),\n"
                        + "    -1\n"
                        + "  )",
                        tldPlus1,
                        preTldPlus1
                    );
                    if(pkey!=-1) {
                        conn.executeUpdate("delete from dns_records where pkey=?", pkey);
                        invalidateList.addTable(
                            conn,
                            SchemaTable.DNS_RECORDS,
                            getBusinessForDNSZone(conn, tldPlus1),
                            getDNSServers(conn),
                            false
                        );
                        updateDNSZoneSerial(conn, invalidateList, tldPlus1);
                    }
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Sets the default TTL for a <code>DNSZone</code>.
     */
    public static void setDNSZoneTTL(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String zone,
        int ttl
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "setDNSZoneTTL(MasterDatabaseConnection,InvalidateList,String,int)", null);
        try {
            // Must be allowed to access this zone
            checkAccessDNSZone(conn, source, "setDNSZoneTTL", zone);
            if (ttl <= 0 || ttl > 24*60*60) {
                throw new SQLException("Illegal TTL value: "+ttl);
            }
            conn.executeUpdate("update dns_zones set ttl=? where zone=?", ttl, zone);
            invalidateList.addTable(
                conn,
                SchemaTable.DNS_ZONES,
                getBusinessForDNSZone(conn, zone),
                getDNSServers(conn),
                false
            );
            updateDNSZoneSerial(conn, invalidateList, zone);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void updateDhcpDnsRecords(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int ipAddress,
        String dhcpAddress
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "updateDhcpDnsRecords(MasterDatabaseConnection,InvalidateList,int,String)", null);
        try {
            // Find the pkeys of the entries that should be changed
            IntList pkeys=conn.executeIntListQuery("select pkey from dns_records where dhcp_address=?", ipAddress);
            
            // Build a list of affected zones
            List<String> zones=new SortedArrayList<String>();
            
            for(int c=0;c<pkeys.size();c++) {
                int pkey=pkeys.getInt(c);
                String zone=getDNSZoneForDNSRecord(conn, pkey);
                if(!zones.contains(zone)) zones.add(zone);
                conn.executeUpdate("update dns_records set destination=? where pkey=?", dhcpAddress, pkey);
            }
            
            // Invalidate the records
            invalidateList.addTable(
                conn,
                SchemaTable.DNS_RECORDS,
                InvalidateList.allBusinesses,
                InvalidateList.allServers,
                false
            );

            // Update the zone serials
            for(int c=0;c<zones.size();c++) {
                updateDNSZoneSerial(
                    conn,
                    invalidateList,
                    zones.get(c)
                );
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void updateDNSZoneSerial(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        String zone
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "updateDNSZoneSerial(MasterDatabaseConnection,InvalidateList,String)", null);
        try {
            // Get the old serial
            long serial=conn.executeLongQuery("select serial from dns_zones where zone=?", zone);

            // Check if already today or higher
            long todaySerial=DNSZone.getCurrentSerial();
            if(serial>=todaySerial) {
                // If so, just increment by one
                serial++;
            } else {
                // Otherwise, set it to today with daily of 01
                serial=todaySerial;
            }

            // Place the serial back in the database
            conn.executeUpdate("update dns_zones set serial=? where zone=?", serial, zone);
            invalidateList.addTable(
                conn,
                SchemaTable.DNS_ZONES,
                InvalidateList.allBusinesses,
                InvalidateList.allServers,
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static void updateReverseDnsIfExists(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        String ip,
        String hostname
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, DNSHandler.class, "updateReverseDnsIfExists(MasterDatabaseConnection,InvalidateList,String,String)", null);
        try {
            String arpaZone=DNSZone.getArpaZoneForIPAddress(ip);
            if(
                conn.executeBooleanQuery(
                    "select (select zone from dns_zones where zone=?) is not null",
                    arpaZone
                )
            ) {
                int pos=ip.lastIndexOf('.');
                String oct4=ip.substring(pos+1);
                if(
                    conn.executeBooleanQuery(
                        "select (select pkey from dns_records where zone=? and domain=? and type='"+DNSType.PTR+"' limit 1) is not null",
                        arpaZone,
                        oct4
                    )
                ) {
                    updateDNSZoneSerial(conn, invalidateList, arpaZone);

                    conn.executeUpdate(
                        "update dns_records set destination=? where zone=? and domain=? and type='"+DNSType.PTR+'\'',
                        hostname+'.',
                        arpaZone,
                        oct4
                    );
                    invalidateList.addTable(conn, SchemaTable.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}
