/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2017, 2018, 2019, 2020, 2021, 2022, 2024  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of aoserv-master.
 *
 * aoserv-master is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * aoserv-master is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with aoserv-master.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoindustries.aoserv.master.dns;

import com.aoapps.collections.IntList;
import com.aoapps.collections.SortedArrayList;
import com.aoapps.dbc.DatabaseAccess.Null;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.net.DomainName;
import com.aoapps.net.InetAddress;
import com.aoapps.tlds.TopLevelDomain;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.dns.Record;
import com.aoindustries.aoserv.client.dns.RecordType;
import com.aoindustries.aoserv.client.dns.Zone;
import com.aoindustries.aoserv.client.dns.ZoneTable;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.net.AppProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.AoservMaster;
import com.aoindustries.aoserv.master.InvalidateList;
import com.aoindustries.aoserv.master.MasterService;
import com.aoindustries.aoserv.master.ObjectFactories;
import com.aoindustries.aoserv.master.PackageHandler;
import com.aoindustries.aoserv.master.RequestSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;

/**
 * Handles all the accesses to the DNS tables.
 *
 * @author  AO Industries, Inc.
 */
// TODO: Move Zone-specific stuff into ZoneService
public final class DnsService implements MasterService {

  /**
   * Creates a new <code>Record</code>.
   */
  public int addRecord(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      String zone,
      String domain,
      String type,
      int priority,
      int weight,
      int port,
      short flag,
      String tag,
      String destination,
      int ttl
  ) throws IOException, SQLException {
    // Must be allowed to access this zone
    checkAccessDnsZone(conn, source, "addRecord", zone);

    // Must have appropriate priority
    if (conn.queryBoolean("select has_priority from dns.\"RecordType\" where type=?", type)) {
      if (priority == Record.NO_PRIORITY) {
        throw new IllegalArgumentException("priority required for type=" + type);
      } else if (priority <= 0) {
        throw new SQLException("Invalid priority: " + priority);
      }
    } else if (priority != Record.NO_PRIORITY) {
      throw new SQLException("No priority allowed for type=" + type);
    }

    // Must have appropriate weight
    if (conn.queryBoolean("select has_weight from dns.\"RecordType\" where type=?", type)) {
      if (weight == Record.NO_WEIGHT) {
        throw new IllegalArgumentException("weight required for type=" + type);
      } else if (weight <= 0) {
        throw new SQLException("Invalid weight: " + weight);
      }
    } else if (weight != Record.NO_WEIGHT) {
      throw new SQLException("No weight allowed for type=" + type);
    }

    // Must have appropriate port
    if (conn.queryBoolean("select has_port from dns.\"RecordType\" where type=?", type)) {
      if (port == Record.NO_PORT) {
        throw new IllegalArgumentException("port required for type=" + type);
      } else if (port < 1 || port > 65535) {
        throw new SQLException("Invalid port: " + port);
      }
    } else if (port != Record.NO_PORT) {
      throw new SQLException("No port allowed for type=" + type);
    }

    // Must have appropriate flag
    if (conn.queryBoolean("select has_flag from dns.\"RecordType\" where type=?", type)) {
      if (flag == Record.NO_FLAG) {
        throw new IllegalArgumentException("flag required for type=" + type);
      } else if (flag < 0 || flag > 0xFF) {
        throw new SQLException("Invalid flag: " + flag);
      }
    } else if (flag != Record.NO_FLAG) {
      throw new SQLException("No flag allowed for type=" + type);
    }

    // Must have appropriate tag
    if (conn.queryBoolean("select has_tag from dns.\"RecordType\" where type=?", type)) {
      if (tag == null) {
        throw new IllegalArgumentException("tag required for type=" + type);
      }
    } else if (tag != null) {
      throw new SQLException("No tag allowed for type=" + type);
    }

    // Must have a valid destination type unless is a TXT entry
    if (!RecordType.TXT.equals(type)) {
      try {
        RecordType.checkDestination(
            type,
            tag,
            destination
        );
      } catch (IllegalArgumentException err) {
        throw new SQLException("Invalid destination: " + err.getMessage());
      }
    }

    // Add the entry
    int recordId = conn.updateInt(
        "INSERT INTO dns.\"Record\" (\n"
            + "  \"zone\",\n"
            + "  \"domain\",\n"
            + "  \"type\",\n"
            + "  priority,\n"
            + "  weight,\n"
            + "  port,\n"
            + "  flag,\n"
            + "  tag,\n"
            + "  destination,\n"
            + "  ttl\n"
            + ") VALUES (?,?,?,?,?,?,?,?,?,?) RETURNING id",
        zone,
        domain,
        type,
        (priority == Record.NO_PRIORITY) ? Null.INTEGER : priority,
        (weight == Record.NO_WEIGHT) ? Null.INTEGER : weight,
        (port == Record.NO_PORT) ? Null.INTEGER : port,
        (flag == Record.NO_FLAG) ? Null.SMALLINT : flag,
        tag,
        destination,
        (ttl == -1) ? Null.INTEGER : ttl
    );
    invalidateList.addTable(conn, Table.TableId.DNS_RECORDS, InvalidateList.allAccounts, InvalidateList.allHosts, false);

    // Update the serial of the zone
    updateDnsZoneSerial(conn, invalidateList, zone);

    // Notify all clients of the update
    return recordId;
  }

  /**
   * Creates a new <code>Zone</code>.
   */
  @SuppressWarnings("deprecation")
  public void addDnsZone(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      Account.Name packageName,
      String zone,
      InetAddress ip,
      int ttl
  ) throws IOException, SQLException {
    // Must be allowed to access this package
    PackageHandler.checkAccessPackage(conn, source, "addDnsZone", packageName);
    if (PackageHandler.isPackageDisabled(conn, packageName)) {
      throw new SQLException("Not allowed to add Zone to disabled Package: " + packageName);
    }
    AoservMaster.checkAccessHostname(conn, source, "addDnsZone", zone);
    // Check the zone format
    List<DomainName> tlds = getTopLevelDomains(conn);
    if (!ZoneTable.checkDnsZone(zone, tlds)) {
      throw new SQLException("Invalid zone: " + zone);
    }

    // Must not be allocated in any way to another account
    AoservMaster.checkAccessHostname(conn, source, "addDnsZone", zone);

    // Add the dns_zone entry
    conn.update(
        "insert into dns.\"Zone\" values(?,?,?,?,?,?)",
        zone,
        zone,
        packageName,
        Zone.DEFAULT_HOSTMASTER,
        Zone.getCurrentSerial(),
        ttl
    );

    // Add the CAA entry defaulting to most protective
    conn.update(
        "insert into dns.\"Record\"(\"zone\", \"domain\", \"type\", flag, tag, destination) values(?,?,?,?,?,?)",
        zone,
        "@",
        RecordType.CAA,
        Zone.DEFAULT_CAA_FLAG,
        Zone.DEFAULT_CAA_TAG,
        Zone.DEFAULT_CAA_VALUE
    );
    conn.update(
        "insert into dns.\"Record\"(\"zone\", \"domain\", \"type\", flag, tag, destination) values(?,?,?,?,?,?)",
        zone,
        "@",
        RecordType.CAA,
        0,
        Record.CAA_TAG_IODEF,
        "mailto:support@aoindustries.com"
    );

    // Add the MX entry
    conn.update(
        "insert into dns.\"Record\"(\"zone\", \"domain\", \"type\", priority, destination) values(?,?,?,?,?)",
        zone,
        "@",
        RecordType.MX,
        Zone.DEFAULT_MX_PRIORITY,
        "mail"
    );

    final String insertRecord = "insert into dns.\"Record\"(\"zone\", \"domain\", \"type\", destination) values(?,?,?,?)";
    // TODO: Take a "mail exchanger" parameter to properly setup the default MX records.
    //       If in this domain, sets up SPF like below.  If outside this domain (ends in .),
    //       sets up MX to the mail exchanger, and CNAME "mail" to the mail exchanger.

    // TODO: Take nameservers from reseller.Brand

    String atype;
    switch (ip.getAddressFamily()) {
      case INET:
        atype = RecordType.A;
        break;
      case INET6:
        atype = RecordType.AAAA;
        break;
      default:
        throw new AssertionError();
    }

    conn.update(insertRecord, zone, "@",    RecordType.NS,  "ns1.aoindustries.com.");
    conn.update(insertRecord, zone, "@",    RecordType.NS,  "ns2.aoindustries.com.");
    conn.update(insertRecord, zone, "@",    RecordType.NS,  "ns3.aoindustries.com.");
    conn.update(insertRecord, zone, "@",    RecordType.NS,  "ns4.aoindustries.com.");
    conn.update(insertRecord, zone, "@",    RecordType.TXT, "v=spf1 a mx -all");
    conn.update(insertRecord, zone, "@",    atype,       ip.toString());
    /*
    conn.update(INSERT_RECORD, zone, "ftp",  aType,       ip.toString());
    conn.update(INSERT_RECORD, zone, "ftp",  RecordType.TXT, "v=spf1 -all");
     */
    conn.update(insertRecord, zone, "mail", atype,       ip.toString());
    // See http://www.openspf.org/FAQ/Common_mistakes#helo "Publish SPF records for HELO names used by your mail servers"
    conn.update(insertRecord, zone, "mail", RecordType.TXT, "v=spf1 a -all");
    conn.update(insertRecord, zone, "www",  atype,       ip.toString());
    // See http://www.openspf.org/FAQ/Common_mistakes#all-domains "Publish null SPF records for your domains that don't send mail"
    conn.update(insertRecord, zone, "www",  RecordType.TXT, "v=spf1 -all");

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.DNS_ZONES, InvalidateList.allAccounts, InvalidateList.allHosts, false);
    invalidateList.addTable(conn, Table.TableId.DNS_RECORDS, InvalidateList.allAccounts, InvalidateList.allHosts, false);
  }

  /**
   * Removes a <code>Record</code>.
   */
  public void removeRecord(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int recordId
  ) throws IOException, SQLException {
    // Must be allowed to access this zone record
    checkAccessRecord(conn, source, "removeRecord", recordId);

    // Get the zone associated with the id
    String zone = getZoneForRecord(conn, recordId);

    // Remove the dns.Record entry
    conn.update("delete from dns.\"Record\" where id=?", recordId);
    invalidateList.addTable(conn, Table.TableId.DNS_RECORDS, InvalidateList.allAccounts, InvalidateList.allHosts, false);

    // Update the serial of the zone
    updateDnsZoneSerial(conn, invalidateList, zone);
  }

  /**
   * Removes a <code>Zone</code>.
   */
  public void removeDnsZone(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      String zone
  ) throws IOException, SQLException {
    // Must be allowed to access this zone
    checkAccessDnsZone(conn, source, "removeDnsZone", zone);

    removeDnsZone(conn, invalidateList, zone);
  }

  /**
   * Removes a <code>Zone</code>.
   */
  public void removeDnsZone(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      String zone
  ) throws IOException, SQLException {
    // Remove the dns.Record entries
    conn.update("delete from dns.\"Record\" where \"zone\"=?", zone);

    // Remove the dns.Zone entry
    conn.update("delete from dns.\"Zone\" where \"zone\"=?", zone);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.DNS_RECORDS, InvalidateList.allAccounts, InvalidateList.allHosts, false);
    invalidateList.addTable(conn, Table.TableId.DNS_ZONES, InvalidateList.allAccounts, InvalidateList.allHosts, false);
  }

  /**
   * Gets the part of the DNS entry before the zone or "@" for the zone itself.
   */
  private static String getPreTld(DomainName hostname, DomainName tld) {
    String hostnameStr = hostname.toLowerCase();
    String tldStr = tld.toLowerCase();
    if (hostnameStr.equals(tldStr)) {
      return "@";
    }
    if (!hostnameStr.endsWith("." + tldStr)) {
      throw new IllegalArgumentException("hostname not in tld: " + hostname + ", " + tld);
    }
    String preTld = hostnameStr.substring(0, hostnameStr.length() - ".".length() - tldStr.length());
    if (preTld.isEmpty()) {
      throw new IllegalArgumentException("Empty preTld: " + preTld);
    }
    return preTld;
  }

  /* Unused 2018-12-02:
  public boolean addRecord(
    DatabaseConnection conn,
    InvalidateList invalidateList,
    DomainName hostname,
    InetAddress ipAddress,
    List<DomainName> tlds
  ) throws IOException, SQLException {
    DomainName tld = ZoneTable.getHostTld(hostname, tlds);
    String zone = tld + ".";
    boolean exists = conn.queryBoolean(
      "select (select zone from dns.\"Zone\" where zone=?) is not null",
      zone
    );
    if (exists) {
      String preTld = getPreTld(hostname, tld);
      exists = conn.queryBoolean(
        "select (select id from dns.\"Record\" where \"zone\"=? and \"type\"='A' and \"domain\"=?) is not null",
        zone,
        preTld
      );
      if (!exists) {
        String atype;
        switch (ipAddress.getAddressFamily()) {
          case INET:
            atype = RecordType.A;
            break;
          case INET6:
            atype = RecordType.AAAA;
            break;
          default:
            throw new AssertionError();
        }
        conn.update(
          "insert into dns.\"Record\" (\"zone\", \"domain\", \"type\", destination) values (?,?,?,?)",
          zone,
          preTld,
          atype,
          ipAddress
        );
        invalidateList.addTable(
          conn,
          Table.TableId.DNS_RECORDS,
          getAccountForDNSZone(conn, zone),
          getDnsLinuxServers(conn),
          false
        );
        updateDnsZoneSerial(conn, invalidateList, zone);
        return true;
      }
    }
    return false;
  }
   */

  private static void checkAccessRecord(DatabaseConnection conn, RequestSource source, String action, int recordId) throws IOException, SQLException {
    if (
        !isDnsAdmin(conn, source)
            && !PackageHandler.canAccessPackage(conn, source, getPackageForRecord(conn, recordId))
    ) {
      String message =
          "currentAdministrator="
              + source.getCurrentAdministrator()
              + " is not allowed to access dns_record: action='"
              + action
              + ", id="
              + recordId;
      throw new SQLException(message);
    }
  }

  public boolean canAccessDnsZone(DatabaseConnection conn, RequestSource source, String zone) throws IOException, SQLException {
    return
        isDnsAdmin(conn, source)
            || PackageHandler.canAccessPackage(conn, source, getPackageForDnsZone(conn, zone));
  }

  private void checkAccessDnsZone(DatabaseConnection conn, RequestSource source, String action, String zone) throws IOException, SQLException {
    if (!canAccessDnsZone(conn, source, zone)) {
      String message =
          "currentAdministrator="
              + source.getCurrentAdministrator()
              + " is not allowed to access dns_zone: action='"
              + action
              + ", zone='"
              + zone
              + '\'';
      throw new SQLException(message);
    }
  }

  /**
   * Admin access to named info is granted if either no server is restricted, or one of the
   * granted servers is a named machine.
   */
  private static boolean isDnsAdmin(
      DatabaseConnection conn,
      RequestSource source
  ) throws IOException, SQLException {
    User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    return mu != null && mu.isDnsAdmin();
  }

  /* Unused 2018-12-02:
  public Account.Name getAccountForRecord(DatabaseConnection conn, int recordId) throws IOException, SQLException {
    return conn.queryObject(
      ObjectFactories.accountingCodeFactory,
      "select pk.accounting from dns.\"Record\" nr, dns.\"Zone\" nz, billing.\"Package\" pk where nr.\"zone\"=nz.\"zone\" and nz.package=pk.\"name\" and nr.id=?",
      recordId
    );
  }
   */

  private static Account.Name getAccountForDnsZone(DatabaseConnection conn, String zone) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select pk.accounting from dns.\"Zone\" nz, billing.\"Package\" pk where nz.package=pk.name and nz.zone=?",
        zone
    );
  }

  private static IntList getDnsLinuxServers(DatabaseConnection conn) throws IOException, SQLException {
    return conn.queryIntList("select distinct server from net.\"Bind\" where app_protocol=? and server in (select server from linux.\"Server\")", AppProtocol.DNS);
  }

  private static final Object dnstldLock = new Object();
  private static List<DomainName> dnstldCache;

  // TODO: Move to a TopLevelDomainService
  /**
   * Gets the contents of the <code>dns.TopLevelDomain</code> table.  Please note
   * that these are only our manually configured entries, and do not contain the
   * full list from {@link TopLevelDomain}.
   *
   * <p>Also, this is a list of effective top-level domains, for the purposes of
   * domain allocation.  This means it includes things like <code>com.au</code>,
   * whereas the {@link TopLevelDomain} only includes <code>au</code>.</p>
   *
   * <p>TODO: Automatically maintain this list from the {@link TopLevelDomain} source, with
   * an "auto" flag.  Add/remove as-needed when auto.</p>
   *
   * <p>TODO: Have a flag "isRegistrable" that enables/disables a domain as being
   * allowed for use by clients.  Something marked isRegistrable and auto should never be removed?
   * Instead of removing auto entries, have a "removed" timestamp showing when it no longer exists?</p>
   *
   * <p>TODO: Allow a comment on each entry, too.</p>
   *
   * <p>TODO: This could replace ForbiddenZones by adding more specific entries, and marking as isRegistrable=false?</p>
   */
  public List<DomainName> getTopLevelDomains(DatabaseConnection conn) throws IOException, SQLException {
    synchronized (dnstldLock) {
      if (dnstldCache == null) {
        dnstldCache = conn.queryList(
            ObjectFactories.domainNameFactory,
            "select domain from dns.\"TopLevelDomain\""
        );
      }
      return dnstldCache;
    }
  }

  private static String getZoneForRecord(DatabaseConnection conn, int recordId) throws IOException, SQLException {
    return conn.queryString("select \"zone\" from dns.\"Record\" where id=?", recordId);
  }

  public boolean isDnsZoneAvailable(DatabaseConnection conn, String zone) throws IOException, SQLException {
    return conn.queryBoolean("select (select zone from dns.\"Zone\" where zone=?) is null", zone);
  }

  private static Account.Name getPackageForRecord(DatabaseConnection conn, int recordId) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select nz.package from dns.\"Record\" nr, dns.\"Zone\" nz where nr.id=? and nr.\"zone\"=nz.\"zone\"",
        recordId
    );
  }

  private static Account.Name getPackageForDnsZone(DatabaseConnection conn, String zone) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select package from dns.\"Zone\" where zone=?",
        zone
    );
  }

  public void invalidateTable(Table.TableId tableId) {
    switch (tableId) {
      case DNS_TLDS:
        synchronized (dnstldLock) {
          dnstldCache = null;
        }
        break;
      default:
        // fall-through
    }
  }

  // TODO: Manage SPF records here
  public void removeUnusedDnsRecord(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      DomainName hostname,
      List<DomainName> tlds
  ) throws IOException, SQLException {
    if (conn.queryBoolean("select (select id from web.\"VirtualHostName\" where hostname=? limit 1) is null", hostname)) {
      DomainName tld = ZoneTable.getHostTld(hostname, tlds);
      String zone = tld + ".";
      if (conn.queryBoolean("select (select zone from dns.\"Zone\" where zone=?) is not null", zone)) {
        String preTld = getPreTld(hostname, tld);
        int deleteCount = conn.update(
            "delete from dns.\"Record\" where\n"
                + "  \"zone\"=?\n"
                + "  and \"type\" in (?,?)\n"
                + "  and \"domain\"=?",
            zone,
            RecordType.A, RecordType.AAAA,
            preTld
        );
        if (deleteCount > 0) {
          invalidateList.addTable(
              conn,
              Table.TableId.DNS_RECORDS,
              getAccountForDnsZone(conn, zone),
              getDnsLinuxServers(conn),
              false
          );
          updateDnsZoneSerial(conn, invalidateList, zone);
        }
      }
    }
  }

  /**
   * Sets the default TTL for a <code>Zone</code>.
   */
  public void setDnsZoneTtl(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      String zone,
      int ttl
  ) throws IOException, SQLException {
    // Must be allowed to access this zone
    checkAccessDnsZone(conn, source, "setDnsZoneTtl", zone);
    if (ttl <= 0 || ttl > 24 * 60 * 60) {
      throw new SQLException("Illegal TTL value: " + ttl);
    }
    conn.update("update dns.\"Zone\" set ttl=? where zone=?", ttl, zone);
    invalidateList.addTable(
        conn,
        Table.TableId.DNS_ZONES,
        getAccountForDnsZone(conn, zone),
        getDnsLinuxServers(conn),
        false
    );
    updateDnsZoneSerial(conn, invalidateList, zone);
  }

  public void updateDhcpDnsRecords(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      int dhcpAddress,
      InetAddress destination
  ) throws IOException, SQLException {
    // Find the ids of the entries that should be changed
    IntList records = conn.queryIntList("select id from dns.\"Record\" where \"dhcpAddress\"=?", dhcpAddress);

    // Build a list of affected zones
    List<String> zones = new SortedArrayList<>();

    for (int c = 0; c < records.size(); c++) {
      int recordId = records.getInt(c);
      String zone = getZoneForRecord(conn, recordId);
      if (!zones.contains(zone)) {
        zones.add(zone);
      }
      conn.update("update dns.\"Record\" set destination=? where id=?", destination, recordId);
    }

    // Invalidate the records
    invalidateList.addTable(conn,
        Table.TableId.DNS_RECORDS,
        InvalidateList.allAccounts,
        InvalidateList.allHosts,
        false
    );

    // Update the zone serials
    for (String zone : zones) {
      updateDnsZoneSerial(
          conn,
          invalidateList,
          zone
      );
    }
  }

  private static void updateDnsZoneSerial(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      String zone
  ) throws IOException, SQLException {
    // Get the old serial
    long serial = conn.queryLong("select serial from dns.\"Zone\" where zone=?", zone);

    // Check if already today or higher
    long todaySerial = Zone.getCurrentSerial();
    if (serial >= todaySerial) {
      // If so, just increment by one
      serial++;
    } else {
      // Otherwise, set it to today with daily of 01
      serial = todaySerial;
    }

    // Place the serial back in the database
    conn.update("update dns.\"Zone\" set serial=? where zone=?", serial, zone);
    invalidateList.addTable(conn,
        Table.TableId.DNS_ZONES,
        InvalidateList.allAccounts,
        InvalidateList.allHosts,
        false
    );
  }

  @SuppressWarnings("deprecation")
  public void updateReverseDnsIfExists(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      InetAddress ip,
      DomainName hostname
  ) throws IOException, SQLException {
    switch (ip.getAddressFamily()) {
      case INET:
        {
          final String netmask;
          final String ipStr = ip.toString();
          if (
              ipStr.startsWith("66.160.183.")
                  || ipStr.startsWith("64.62.174.")
          ) {
            netmask = "255.255.255.0";
          } else if (ipStr.startsWith("64.71.144.")) {
            netmask = "255.255.255.128";
          } else {
            netmask = null;
          }
          if (netmask != null) {
            String arpaZone = Zone.getArpaZoneForIpAddress(ip, netmask);
            if (
                conn.queryBoolean(
                    "select (select zone from dns.\"Zone\" where zone=?) is not null",
                    arpaZone
                )
            ) {
              int pos = ipStr.lastIndexOf('.');
              String oct4 = ipStr.substring(pos + 1);
              if (
                  conn.queryBoolean(
                      "select (select id from dns.\"Record\" where \"zone\"=? and \"domain\"=? and \"type\"=? limit 1) is not null",
                      arpaZone,
                      oct4,
                      RecordType.PTR
                  )
              ) {
                updateDnsZoneSerial(conn, invalidateList, arpaZone);

                conn.update(
                    "update dns.\"Record\" set destination=? where \"zone\"=? and \"domain\"=? and \"type\"=?",
                    hostname.toString() + '.',
                    arpaZone,
                    oct4,
                    RecordType.PTR
                );
                invalidateList.addTable(conn, Table.TableId.DNS_RECORDS, InvalidateList.allAccounts, InvalidateList.allHosts, false);
              }
            }
          }
          break;
        }
      case INET6:
        throw new NotImplementedException();
      default:
        throw new AssertionError();
    }
  }
}
