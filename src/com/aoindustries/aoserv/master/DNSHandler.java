/*
 * Copyright 2001-2013 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.DNSRecord;
import com.aoindustries.aoserv.client.DNSType;
import com.aoindustries.aoserv.client.DNSZone;
import com.aoindustries.aoserv.client.DNSZoneTable;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.Protocol;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.DomainName;
import com.aoindustries.aoserv.client.validator.InetAddress;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.CronJobScheduleMode;
import com.aoindustries.cron.Schedule;
import com.aoindustries.util.logging.ProcessTimer;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.WrappedSQLException;
import com.aoindustries.util.IntList;
import com.aoindustries.util.SortedArrayList;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The <code>DNSHandler</code> handles all the accesses to the DNS tables.
 *
 * @author  AO Industries, Inc.
 */
final public class DNSHandler implements CronJob {

    private static final Logger logger = LogFactory.getLogger(DNSHandler.class);

    /**
     * The maximum time for a processing pass.
     */
    private static final long TIMER_MAX_TIME=20L*60*1000;

    /**
     * The interval in which the administrators will be reminded.
     */
    private static final long TIMER_REMINDER_INTERVAL=6L*60*60*1000;

    private static boolean started=false;

    public static void start() {
        synchronized(System.out) {
            if(!started) {
                System.out.print("Starting DNSHandler: ");
                CronDaemon.addCronJob(new DNSHandler(), logger);
                started=true;
                System.out.println("Done");
            }
        }
    }
    
    private DNSHandler() {
    }

    private static final Schedule schedule = new Schedule() {
        /**
         * Runs at 6:12 am on the 1st, 7th, 13th, 19th, and 25th
         */
		@Override
        public boolean isCronJobScheduled(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
            return
                minute==12
                && hour==6
                && (
                    dayOfMonth==1
                    || dayOfMonth==7
                    || dayOfMonth==13
                    || dayOfMonth==19
                    || dayOfMonth==25
                )
            ;
        }
    };

	@Override
    public Schedule getCronJobSchedule() {
        return schedule;
    }

	@Override
    public CronJobScheduleMode getCronJobScheduleMode() {
        return CronJobScheduleMode.SKIP;
    }

	@Override
    public String getCronJobName() {
        return "DNSHandler";
    }

	@Override
    public int getCronJobThreadPriority() {
        return Thread.NORM_PRIORITY-1;
    }

	@Override
    public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
        try {
            ProcessTimer timer=new ProcessTimer(
                logger,
                MasterServer.getRandom(),
                DNSHandler.class.getName(),
                "runCronJob",
                "DNSHandler - Whois History",
                "Looking up whois and cleaning old records",
                TIMER_MAX_TIME,
                TIMER_REMINDER_INTERVAL
            );
            try {
                MasterServer.executorService.submit(timer);

                // Start the transaction
                InvalidateList invalidateList=new InvalidateList();
                DatabaseConnection conn=MasterDatabase.getDatabase().createDatabaseConnection();
                try {
                    boolean connRolledBack=false;
                    try {
                        /*
                         * Remove old records first
                         */
                        //  Open account that have balance <= $0.00 and entry is older than one year
                        int updated = conn.executeUpdate(
                            "delete from whois_history where pkey in (\n"
                            + "  select\n"
                            + "    wh.pkey\n"
                            + "  from\n"
                            + "    whois_history wh\n"
                            + "    inner join businesses bu on wh.accounting=bu.accounting\n"
                            + "    left outer join account_balances ab on bu.accounting=ab.accounting"
                            + "  where\n"
                            // entry is older than one year
                            + "    (now()-wh.time)>'1 year'::interval\n"
                            // open account
                            + "    and bu.canceled is null\n"
                            // balance is <= $0.00
                            + "    and (ab.accounting is null or ab.balance<='0.00'::decimal(9,2))"
                            + ")"
                        );
                        if(updated>0) invalidateList.addTable(conn, SchemaTable.TableID.WHOIS_HISTORY, InvalidateList.allBusinesses, InvalidateList.allServers, false);

                        // Closed account that have a balance of $0.00, has not had any accounting transactions for one year, and entry is older than one year
                        updated = conn.executeUpdate(
                            "delete from whois_history where pkey in (\n"
                            + "  select\n"
                            + "    wh.pkey\n"
                            + "  from\n"
                            + "    whois_history wh\n"
                            + "    inner join businesses bu on wh.accounting=bu.accounting\n"
                            + "    left outer join account_balances ab on bu.accounting=ab.accounting"
                            + "  where\n"
                            // entry is older than one year
                            + "    (now()-wh.time)>'1 year'::interval\n"
                            // closed account
                            + "    and bu.canceled is not null\n"
                            // has not had any accounting transactions for one year
                            + "    and (select tr.transid from transactions tr where bu.accounting=tr.accounting and tr.time>=(now()-'1 year'::interval) limit 1) is null\n"
                            // balance is $0.00
                            + "    and (ab.accounting is null or ab.balance='0.00'::decimal(9,2))"
                            + ")"
                        );
                        if(updated>0) invalidateList.addTable(conn, SchemaTable.TableID.WHOIS_HISTORY, InvalidateList.allBusinesses, InvalidateList.allServers, false);

                        /*
                         * The add new records
                         */
                        // Get the set of unique accounting, zone combinations in the system
                        Set<AccountingAndZone> topLevelZones = getBusinessesAndTopLevelZones(conn);

                        // Perform the whois lookups once per unique zone
                        Map<String,String> whoisOutputs = new HashMap<>(topLevelZones.size()*4/3+1);
                        for(AccountingAndZone aaz : topLevelZones) {
                            String zone = aaz.getZone();
                            if(!whoisOutputs.containsKey(zone)) {
                                String whoisOutput;
                                try {
                                    whoisOutput = getWhoisOutput(zone);
                                } catch(IOException err) {
                                    whoisOutput = err.toString();
                                }
                                whoisOutputs.put(zone, whoisOutput);
                            }
                        }

                        // update database
                        for(AccountingAndZone aaz : topLevelZones) {
                            String accounting = aaz.getAccounting();
                            String zone = aaz.getZone();
                            String whoisOutput = whoisOutputs.get(zone);
                            conn.executeUpdate("insert into whois_history (accounting, zone, whois_output) values(?,?,?)", accounting, zone, whoisOutput);
                            invalidateList.addTable(conn, SchemaTable.TableID.WHOIS_HISTORY, InvalidateList.allBusinesses, InvalidateList.allServers, false);
                        }
                    } catch(RuntimeException err) {
                        if(conn.rollback()) {
                            connRolledBack=true;
                            invalidateList=null;
                        }
                        throw err;
                    } catch(IOException err) {
                        if(conn.rollback()) {
                            connRolledBack=true;
                            invalidateList=null;
                        }
                        throw err;
                    } catch(SQLException err) {
                        if(conn.rollbackAndClose()) {
                            connRolledBack=true;
                            invalidateList=null;
                        }
                        throw err;
                    } finally {
                        if(!connRolledBack && !conn.isClosed()) conn.commit();
                    }
                } finally {
                    conn.releaseConnection();
                }
                if(invalidateList!=null) MasterServer.invalidateTables(invalidateList, null);
            } finally {
                timer.finished();
            }
        } catch(ThreadDeath TD) {
            throw TD;
        } catch(Throwable T) {
            logger.log(Level.SEVERE, null, T);
        }
    }

    /**
     * Performs a whois lookup for a zone.  This is not cross-platform capable at this time.
     */
    public static String getWhoisOutput(String zone) throws IOException {
        Process P = Runtime.getRuntime().exec(new String[] {"/usr/bin/whois", zone});
        try {
            InputStream in = new BufferedInputStream(P.getInputStream());
            try {
                StringBuilder SB = new StringBuilder();
                int c;
                while((c=in.read())!=-1) SB.append((char)c);
                return SB.toString();
            } finally {
                in.close();
            }
        } finally {
            try {
                int retVal = P.waitFor();
                if(retVal!=0) throw new IOException("/usr/bin/whois '"+zone+"' returned with non-zero value: "+retVal);
            } catch(InterruptedException err) {
                InterruptedIOException ioErr = new InterruptedIOException("Interrupted while waiting for whois to complete");
                ioErr.initCause(err);
                throw ioErr;
            }
        }
    }

    /**
     * Gets the set of all unique business accounting code and top level domain (zone) pairs.
     *
     * @see  DNSZoneTable#getHostTLD
     */
    public static Set<AccountingAndZone> getBusinessesAndTopLevelZones(DatabaseConnection conn) throws IOException, SQLException {
        List<DomainName> tlds = getDNSTLDs(conn);

        Connection dbConn = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true);
        try {
            Statement stmt = dbConn.createStatement();
            try {
                String sql = "select distinct\n"
                           + "  pk.accounting as accounting,\n"
                           + "  dz.zone as zone\n"
                           + "from\n"
                           + "  dns_zones dz\n"
                           + "  inner join packages pk on dz.package=pk.name\n"
                           + "where\n"
                           + "  dz.zone not like '%.in-addr.arpa'\n"
                           + "union select distinct\n"
                           + "  pk.accounting as accounting,\n"
                           + "  ed.domain||'.' as zone\n"
                           + "from\n"
                           + "  email_domains ed\n"
                           + "  inner join packages pk on ed.package=pk.name\n"
                           + "union select distinct\n"
                           + "  pk.accounting as accounting,\n"
                           + "  hsu.hostname||'.' as zone\n"
                           + "from\n"
                           + "  httpd_site_urls hsu\n"
                           + "  inner join httpd_site_binds hsb on hsu.httpd_site_bind=hsb.pkey\n"
                           + "  inner join httpd_sites hs on hsb.httpd_site=hs.pkey\n"
                           + "  inner join packages pk on hs.package=pk.name\n"
                           + "  inner join ao_servers ao on hs.ao_server=ao.server\n"
                           + "where\n"
                           // Is not the test URL
                           + "  hsu.hostname!=(hs.site_name || '.' || ao.hostname)";
                try {
                    ResultSet results = stmt.executeQuery(sql);
                    try {
                        Set<AccountingAndZone> aazs = new HashSet<>();
                        while(results.next()) {
                            String accounting = results.getString(1);
                            String zone = results.getString(2);
                            String tld;
                            try {
                                tld = DNSZoneTable.getHostTLD(zone, tlds);
                            } catch(IllegalArgumentException err) {
                                logger.log(Level.WARNING, null, err);
                                tld = zone;
                            }
                            AccountingAndZone aaz = new AccountingAndZone(accounting, tld);
                            if(!aazs.contains(aaz)) aazs.add(aaz);
                        }
                        return aazs;
                    } finally {
                        results.close();
                    }
                } catch(SQLException err) {
                    // Include the SQL in the exception
                    throw new WrappedSQLException(err, sql);
                }
            } finally {
                stmt.close();
            }
        } finally {
            conn.releaseConnection();
        }
    }

    /**
     * Gets the whois output for the specific whois_history record.
     */
    public static String getWhoisHistoryOutput(DatabaseConnection conn, RequestSource source, int pkey) throws IOException, SQLException {
        AccountingCode accounting = getBusinessForWhoisHistory(conn, pkey);
        BusinessHandler.checkAccessBusiness(conn, source, "getWhoisHistoryOutput", accounting);
        return conn.executeStringQuery("select whois_output from whois_history where pkey=?", pkey);
    }

    /**
     * Creates a new <code>DNSRecord</code>.
     */
    public static int addDNSRecord(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String zone,
        String domain,
        String type,
        int mx_priority,
        String destination,
        int ttl
    ) throws IOException, SQLException {
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

        // Must have a valid destination type unless is a TXT entry
        if(!DNSType.TXT.equals(type)) {
            try {
                DNSType.checkDestination(
                    type,
                    destination
                );
            } catch(IllegalArgumentException err) {
                throw new SQLException("Invalid destination: "+err.getMessage());
            }
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
            pstmt.executeUpdate();
        } catch(SQLException err) {
            System.err.println("Error from query: "+pstmt.toString());
            throw err;
        } finally {
            pstmt.close();
        }
        invalidateList.addTable(conn, SchemaTable.TableID.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);

        // Update the serial of the zone
        updateDNSZoneSerial(conn, invalidateList, zone);

        // Notify all clients of the update
        return pkey;
    }

    /**
     * Creates a new <code>DNSZone</code>.
     */
    public static void addDNSZone(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String packageName,
        String zone,
        InetAddress ip,
        int ttl
    ) throws IOException, SQLException {
        // Must be allowed to access this package
        PackageHandler.checkAccessPackage(conn, source, "addDNSZone", packageName);
        if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Not allowed to add DNSZone to disabled Package: "+packageName);
        MasterServer.checkAccessHostname(conn, source, "addDNSZone", zone);
        // Check the zone format
        List<DomainName> tlds=getDNSTLDs(conn);
        if(!DNSZoneTable.checkDNSZone(zone, tlds)) throw new SQLException("Invalid zone: "+zone);

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
            pstmt.executeUpdate();
        } finally {
            pstmt.close();
        }

        pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into dns_records(zone, domain, type, destination) values(?,?,?,?)");
        try {
            // Add the ns1.aoindustries.com name server
            pstmt.setString(1, zone);
            pstmt.setString(2, "@");
            pstmt.setString(3, DNSType.NS);
            pstmt.setString(4, "ns1.aoindustries.com.");
            pstmt.executeUpdate();

            // Add the ns2.aoindustries.com name server
            pstmt.setString(1, zone);
            pstmt.setString(2, "@");
            pstmt.setString(3, DNSType.NS);
            pstmt.setString(4, "ns2.aoindustries.com.");
            pstmt.executeUpdate();

            // Add the ns3.aoindustries.com name server
            pstmt.setString(1, zone);
            pstmt.setString(2, "@");
            pstmt.setString(3, DNSType.NS);
            pstmt.setString(4, "ns3.aoindustries.com.");
            pstmt.executeUpdate();

            // Add the ns4.aoindustries.com name server
            pstmt.setString(1, zone);
            pstmt.setString(2, "@");
            pstmt.setString(3, DNSType.NS);
            pstmt.setString(4, "ns4.aoindustries.com.");
            pstmt.executeUpdate();

            String aType = ip.isIPv6() ? DNSType.AAAA : DNSType.A;

            // Add the domain IP
            pstmt.setString(1, zone);
            pstmt.setString(2, "@");
            pstmt.setString(3, aType);
            pstmt.setString(4, ip.toString());
            pstmt.executeUpdate();

            // Add the ftp IP
            /*
            pstmt.setString(1, zone);
            pstmt.setString(2, "ftp");
            pstmt.setString(3, aType);
            pstmt.setString(4, ip.toString());
            pstmt.executeUpdate();
             */

            // Add the mail IP
            pstmt.setString(1, zone);
            pstmt.setString(2, "mail");
            pstmt.setString(3, aType);
            pstmt.setString(4, ip.toString());
            pstmt.executeUpdate();

            // Add the www IP
            pstmt.setString(1, zone);
            pstmt.setString(2, "www");
            pstmt.setString(3, aType);
            pstmt.setString(4, ip.toString());
            pstmt.executeUpdate();
        } finally {
            pstmt.close();
        }

        // Notify all clients of the update
        invalidateList.addTable(conn, SchemaTable.TableID.DNS_ZONES, InvalidateList.allBusinesses, InvalidateList.allServers, false);
        invalidateList.addTable(conn, SchemaTable.TableID.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
    }

    /**
     * Removes a <code>DNSRecord</code>.
     */
    public static void removeDNSRecord(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        // Must be allowed to access this zone record
        checkAccessDNSRecord(conn, source, "removeDNSRecord", pkey);

        // Get the zone associated with the pkey
        String zone=getDNSZoneForDNSRecord(conn, pkey);

        // Remove the dns_records entry
        PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("delete from dns_records where pkey=?");
        try {
            pstmt.setInt(1, pkey);
            pstmt.executeUpdate();
        } finally {
            pstmt.close();
        }
        invalidateList.addTable(conn, SchemaTable.TableID.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);

        // Update the serial of the zone
        updateDNSZoneSerial(conn, invalidateList, zone);
    }

    /**
     * Removes a <code>DNSZone</code>.
     */
    public static void removeDNSZone(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String zone
    ) throws IOException, SQLException {
        // Must be allowed to access this zone
        checkAccessDNSZone(conn, source, "removeDNSZone", zone);

        removeDNSZone(conn, invalidateList, zone);
    }

    /**
     * Removes a <code>DNSZone</code>.
     */
    public static void removeDNSZone(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        String zone
    ) throws IOException, SQLException {
        // Remove the dns_records entries
        PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("delete from dns_records where zone=?");
        try {
            pstmt.setString(1, zone);
            pstmt.executeUpdate();
        } finally {
            pstmt.close();
        }

        // Remove the dns_zones entry
        pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("delete from dns_zones where zone=?");
        try {
            pstmt.setString(1, zone);
            pstmt.executeUpdate();
        } finally {
            pstmt.close();
        }

        // Notify all clients of the update
        invalidateList.addTable(conn, SchemaTable.TableID.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
        invalidateList.addTable(conn, SchemaTable.TableID.DNS_ZONES, InvalidateList.allBusinesses, InvalidateList.allServers, false);
    }

    public static boolean addDNSRecord(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        String hostname,
        String ipAddress,
        List<DomainName> tlds
    ) throws IOException, SQLException {
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
                    SchemaTable.TableID.DNS_RECORDS,
                    getBusinessForDNSZone(conn, tldPlus1),
                    getDNSAOServers(conn),
                    false
                );
                updateDNSZoneSerial(conn, invalidateList, tldPlus1);
                return true;
            }
        }
        return false;
    }

    public static void checkAccessDNSRecord(DatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
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
            throw new SQLException(message);
        }
    }

    public static boolean canAccessDNSZone(DatabaseConnection conn, RequestSource source, String zone) throws IOException, SQLException {
        return
            isDNSAdmin(conn, source)
            || PackageHandler.canAccessPackage(conn, source, getPackageForDNSZone(conn, zone))
        ;
    }

    public static void checkAccessDNSZone(DatabaseConnection conn, RequestSource source, String action, String zone) throws IOException, SQLException {
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
            throw new SQLException(message);
        }
    }

    /**
     * Admin access to named info is granted if either no server is restricted, or one of the
     * granted servers is a named machine.
     */
    public static boolean isDNSAdmin(
        DatabaseConnection conn,
        RequestSource source
    ) throws IOException, SQLException {
        MasterUser mu=MasterServer.getMasterUser(conn, source.getUsername());
        return mu!=null && mu.isDNSAdmin();
    }

    public static AccountingCode getBusinessForDNSRecord(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeObjectQuery(
            ObjectFactories.accountingCodeFactory,
            "select pk.accounting from dns_records nr, dns_zones nz, packages pk where nr.zone=nz.zone and nz.package=pk.name and nr.pkey=?",
            pkey
        );
    }

    public static AccountingCode getBusinessForDNSZone(DatabaseConnection conn, String zone) throws IOException, SQLException {
        return conn.executeObjectQuery(
            ObjectFactories.accountingCodeFactory,
            "select pk.accounting from dns_zones nz, packages pk where nz.package=pk.name and nz.zone=?",
            zone
        );
    }

    public static AccountingCode getBusinessForWhoisHistory(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeObjectQuery(
            ObjectFactories.accountingCodeFactory,
            "select accounting from whois_history where pkey=?",
            pkey
        );
    }

    public static IntList getDNSAOServers(DatabaseConnection conn) throws IOException, SQLException {
        return conn.executeIntListQuery("select distinct server from net_binds where app_protocol=? and server in (select server from ao_servers)", Protocol.DNS);
    }

    private static final Object dnstldLock=new Object();
    private static List<DomainName> dnstldCache;
    public static List<DomainName> getDNSTLDs(DatabaseConnection conn) throws IOException, SQLException {
        synchronized(dnstldLock) {
            if(dnstldCache==null) {
                dnstldCache=conn.executeObjectCollectionQuery(
                    new ArrayList<DomainName>(),
                    ObjectFactories.domainNameFactory,
                    "select domain from dns_tlds"
                );
            }
            return dnstldCache;
        }
    }
    
    public static String getDNSZoneForDNSRecord(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeStringQuery("select zone from dns_records where pkey=?", pkey);
    }

    public static boolean isDNSZoneAvailable(DatabaseConnection conn, String zone) throws IOException, SQLException {
        return conn.executeBooleanQuery("select (select zone from dns_zones where zone=?) is null", zone);
    }

    public static String getPackageForDNSRecord(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeStringQuery("select nz.package from dns_records nr, dns_zones nz where nr.pkey=? and nr.zone=nz.zone", pkey);
    }

    public static String getPackageForDNSZone(DatabaseConnection conn, String zone) throws IOException, SQLException {
        return conn.executeStringQuery("select package from dns_zones where zone=?", zone);
    }

    public static void invalidateTable(SchemaTable.TableID tableID) {
        switch(tableID) {
            case DNS_TLDS :
                synchronized(dnstldLock) {
                    dnstldCache=null;
                }
                break;
        }
    }

    public static void removeUnusedDNSRecord(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        String hostname,
        List<DomainName> tlds
    ) throws IOException, SQLException {
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
                        SchemaTable.TableID.DNS_RECORDS,
                        getBusinessForDNSZone(conn, tldPlus1),
                        getDNSAOServers(conn),
                        false
                    );
                    updateDNSZoneSerial(conn, invalidateList, tldPlus1);
                }
            }
        }
    }

    /**
     * Sets the default TTL for a <code>DNSZone</code>.
     */
    public static void setDNSZoneTTL(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String zone,
        int ttl
    ) throws IOException, SQLException {
        // Must be allowed to access this zone
        checkAccessDNSZone(conn, source, "setDNSZoneTTL", zone);
        if (ttl <= 0 || ttl > 24*60*60) {
            throw new SQLException("Illegal TTL value: "+ttl);
        }
        conn.executeUpdate("update dns_zones set ttl=? where zone=?", ttl, zone);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.DNS_ZONES,
            getBusinessForDNSZone(conn, zone),
            getDNSAOServers(conn),
            false
        );
        updateDNSZoneSerial(conn, invalidateList, zone);
    }

    public static void updateDhcpDnsRecords(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        int ipAddress,
        String dhcpAddress
    ) throws IOException, SQLException {
        // Find the pkeys of the entries that should be changed
        IntList pkeys=conn.executeIntListQuery("select pkey from dns_records where dhcp_address=?", ipAddress);

        // Build a list of affected zones
        List<String> zones=new SortedArrayList<>();

        for(int c=0;c<pkeys.size();c++) {
            int pkey=pkeys.getInt(c);
            String zone=getDNSZoneForDNSRecord(conn, pkey);
            if(!zones.contains(zone)) zones.add(zone);
            conn.executeUpdate("update dns_records set destination=? where pkey=?", dhcpAddress, pkey);
        }

        // Invalidate the records
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.DNS_RECORDS,
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
    }

    public static void updateDNSZoneSerial(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        String zone
    ) throws IOException, SQLException {
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
            SchemaTable.TableID.DNS_ZONES,
            InvalidateList.allBusinesses,
            InvalidateList.allServers,
            false
        );
    }
    
    public static void updateReverseDnsIfExists(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        String ip,
        DomainName hostname
    ) throws IOException, SQLException {
        String netmask;
        if(
            ip.startsWith("66.160.183.")
            || ip.startsWith("64.62.174.")
        ) {
            netmask = "255.255.255.0";
        } else if(ip.startsWith("64.71.144.")) {
            netmask = "255.255.255.128";
        } else {
            netmask = null;
        }
        if(netmask!=null) {
            String arpaZone=DNSZone.getArpaZoneForIPAddress(ip, netmask);
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
                        hostname.toString()+'.',
                        arpaZone,
                        oct4
                    );
                    invalidateList.addTable(conn, SchemaTable.TableID.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
                }
            }
        }
    }
    
    public static class AccountingAndZone {

        final private String accounting;
        final private String zone;
        
        public AccountingAndZone(String accounting, String zone) {
            this.accounting = accounting;
            this.zone = zone;
        }
        
        public String getAccounting() {
            return accounting;
        }
        
        public String getZone() {
            return zone;
        }

        @Override
        public int hashCode() {
            return accounting.hashCode() ^ zone.hashCode();
        }
        
        @Override
        public boolean equals(Object O) {
            if(O==null) return false;
            if(!(O instanceof AccountingAndZone)) return false;
            AccountingAndZone other = (AccountingAndZone)O;
            return accounting.equals(other.accounting) && zone.equals(other.zone);
        }
        
        @Override
        public String toString() {
            return accounting+'|'+zone;
        }
    }
}