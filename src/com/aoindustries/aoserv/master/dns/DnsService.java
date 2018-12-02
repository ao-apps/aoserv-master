/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.dns;

import com.aoindustries.aoserv.client.dns.Record;
import com.aoindustries.aoserv.client.dns.RecordType;
import com.aoindustries.aoserv.client.dns.Zone;
import com.aoindustries.aoserv.client.dns.ZoneTable;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.net.AppProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.master.InvalidateList;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.MasterService;
import com.aoindustries.aoserv.master.ObjectFactories;
import com.aoindustries.aoserv.master.PackageHandler;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.dbc.DatabaseAccess.Null;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.lang.NotImplementedException;
import com.aoindustries.net.DomainName;
import com.aoindustries.net.InetAddress;
import com.aoindustries.util.IntList;
import com.aoindustries.util.SortedArrayList;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles all the accesses to the DNS tables.
 *
 * @author  AO Industries, Inc.
 */
final public class DnsService implements MasterService {

	/**
	 * Creates a new <code>Record</code>.
	 */
	public int addDNSRecord(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		String zone,
		String domain,
		String type,
		int priority,
		int weight,
		int port,
		String destination,
		int ttl
	) throws IOException, SQLException {
		// Must be allowed to access this zone
		checkAccessDNSZone(conn, source, "addDNSRecord", zone);

		// Must have appropriate priority
		if(conn.executeBooleanQuery("select has_priority from dns.\"RecordType\" where type=?", type)) {
			if(priority == Record.NO_PRIORITY) throw new IllegalArgumentException("priority required for type=" + type);
			else if(priority<=0) throw new SQLException("Invalid priority: " + priority);
		} else {
			if(priority != Record.NO_PRIORITY) throw new SQLException("No priority allowed for type="+type);
		}

		// Must have appropriate weight
		if(conn.executeBooleanQuery("select has_weight from dns.\"RecordType\" where type=?", type)) {
			if(weight == Record.NO_WEIGHT) throw new IllegalArgumentException("weight required for type=" + type);
			else if(weight<=0) throw new SQLException("Invalid weight: " + weight);
		} else {
			if(weight != Record.NO_WEIGHT) throw new SQLException("No weight allowed for type="+type);
		}

		// Must have appropriate port
		if(conn.executeBooleanQuery("select has_port from dns.\"RecordType\" where type=?", type)) {
			if(port == Record.NO_PORT) throw new IllegalArgumentException("port required for type=" + type);
			else if(port < 1 || port > 65535) throw new SQLException("Invalid port: " + port);
		} else {
			if(port != Record.NO_PORT) throw new SQLException("No port allowed for type="+type);
		}

		// Must have a valid destination type unless is a TXT entry
		if(!RecordType.TXT.equals(type)) {
			try {
				RecordType.checkDestination(
					type,
					destination
				);
			} catch(IllegalArgumentException err) {
				throw new SQLException("Invalid destination: "+err.getMessage());
			}
		}

		// Add the entry
		int id = conn.executeIntUpdate(
			"INSERT INTO dns.\"Record\" (\n"
			+ "  \"zone\",\n"
			+ "  \"domain\",\n"
			+ "  \"type\",\n"
			+ "  priority,\n"
			+ "  weight,\n"
			+ "  port,\n"
			+ "  destination,\n"
			+ "  ttl\n"
			+ ") VALUES (?,?,?,?,?,?,?,?) RETURNING id",
			zone,
			domain,
			type,
			(priority == Record.NO_PRIORITY) ? Null.INTEGER : priority,
			(weight == Record.NO_WEIGHT) ? Null.INTEGER : weight,
			(port == Record.NO_PORT) ? Null.INTEGER : port,
			destination,
			(ttl == -1) ? Null.INTEGER : ttl
		);
		invalidateList.addTable(conn, Table.TableID.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);

		// Update the serial of the zone
		updateDNSZoneSerial(conn, invalidateList, zone);

		// Notify all clients of the update
		return id;
	}

	/**
	 * Creates a new <code>Zone</code>.
	 */
	public void addDNSZone(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		AccountingCode packageName,
		String zone,
		InetAddress ip,
		int ttl
	) throws IOException, SQLException {
		// Must be allowed to access this package
		PackageHandler.checkAccessPackage(conn, source, "addDNSZone", packageName);
		if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Not allowed to add Zone to disabled Package: "+packageName);
		MasterServer.checkAccessHostname(conn, source, "addDNSZone", zone);
		// Check the zone format
		List<DomainName> tlds=getDNSTLDs(conn);
		if(!ZoneTable.checkDNSZone(zone, tlds)) throw new SQLException("Invalid zone: "+zone);

		// Must not be allocated in any way to another account
		MasterServer.checkAccessHostname(conn, source, "addDNSZone", zone);

		// Add the dns_zone entry
		conn.executeUpdate(
			"insert into dns.\"Zone\" values(?,?,?,?,?,?)",
			zone,
			zone,
			packageName,
			Zone.DEFAULT_HOSTMASTER,
			Zone.getCurrentSerial(),
			ttl
		);

		// Add the MX entry
		conn.executeUpdate(
			"insert into dns.\"Record\"(\"zone\", \"domain\", \"type\", priority, destination) values(?,?,?,?,?)",
			zone,
			"@",
			RecordType.MX,
			Zone.DEFAULT_MX_PRIORITY,
			"mail"
		);

		final String INSERT_RECORD = "insert into dns.\"Record\"(\"zone\", \"domain\", \"type\", destination) values(?,?,?,?)";
		// TODO: Take a "mail exchanger" parameter to properly setup the default MX records.
		//       If in this domain, sets up SPF like below.  If outside this domain (ends in .),
		//       sets up MX to the mail exchanger, and CNAME "mail" to the mail exchanger.

		// TODO: Take nameservers from reseller.Brand

		String aType;
		switch(ip.getAddressFamily()) {
			case INET :
				aType = RecordType.A;
				break;
			case INET6 :
				aType = RecordType.AAAA;
				break;
			default :
				throw new AssertionError();
		}

		conn.executeUpdate(INSERT_RECORD, zone, "@",    RecordType.NS,  "ns1.aoindustries.com.");
		conn.executeUpdate(INSERT_RECORD, zone, "@",    RecordType.NS,  "ns2.aoindustries.com.");
		conn.executeUpdate(INSERT_RECORD, zone, "@",    RecordType.NS,  "ns3.aoindustries.com.");
		conn.executeUpdate(INSERT_RECORD, zone, "@",    RecordType.NS,  "ns4.aoindustries.com.");
		conn.executeUpdate(INSERT_RECORD, zone, "@",    RecordType.TXT, "v=spf1 a mx -all");
		conn.executeUpdate(INSERT_RECORD, zone, "@",    aType,       ip.toString());
		/*
		conn.executeUpdate(INSERT_RECORD, zone, "ftp",  aType,       ip.toString());
		conn.executeUpdate(INSERT_RECORD, zone, "ftp",  RecordType.TXT, "v=spf1 -all");
		 */
		conn.executeUpdate(INSERT_RECORD, zone, "mail", aType,       ip.toString());
		// See http://www.openspf.org/FAQ/Common_mistakes#helo "Publish SPF records for HELO names used by your mail servers"
		conn.executeUpdate(INSERT_RECORD, zone, "mail", RecordType.TXT, "v=spf1 a -all");
		conn.executeUpdate(INSERT_RECORD, zone, "www",  aType,       ip.toString());
		// See http://www.openspf.org/FAQ/Common_mistakes#all-domains "Publish null SPF records for your domains that don't send mail"
		conn.executeUpdate(INSERT_RECORD, zone, "www",  RecordType.TXT, "v=spf1 -all");

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.DNS_ZONES, InvalidateList.allBusinesses, InvalidateList.allServers, false);
		invalidateList.addTable(conn, Table.TableID.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
	}

	/**
	 * Removes a <code>Record</code>.
	 */
	public void removeDNSRecord(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id
	) throws IOException, SQLException {
		// Must be allowed to access this zone record
		checkAccessDNSRecord(conn, source, "removeDNSRecord", id);

		// Get the zone associated with the id
		String zone=getDNSZoneForDNSRecord(conn, id);

		// Remove the dns.Record entry
		conn.executeUpdate("delete from dns.\"Record\" where id=?", id);
		invalidateList.addTable(conn, Table.TableID.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);

		// Update the serial of the zone
		updateDNSZoneSerial(conn, invalidateList, zone);
	}

	/**
	 * Removes a <code>Zone</code>.
	 */
	public void removeDNSZone(
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
	 * Removes a <code>Zone</code>.
	 */
	public void removeDNSZone(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		String zone
	) throws IOException, SQLException {
		// Remove the dns.Record entries
		conn.executeUpdate("delete from dns.\"Record\" where \"zone\"=?", zone);

		// Remove the dns.Zone entry
		conn.executeUpdate("delete from dns.\"Zone\" where \"zone\"=?", zone);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
		invalidateList.addTable(conn, Table.TableID.DNS_ZONES, InvalidateList.allBusinesses, InvalidateList.allServers, false);
	}

	/**
	 * Gets the part of the DNS entry before the zone or "@" for the zone itself.
	 */
	private static String getPreTld(DomainName hostname, DomainName tld) {
		String hostnameStr = hostname.toLowerCase();
		String tldStr = tld.toLowerCase();
		if(hostnameStr.equals(tldStr)) {
			return "@";
		}
		if(!hostnameStr.endsWith("." + tldStr)) {
			throw new IllegalArgumentException("hostname not in tld: " + hostname + ", " + tld);
		}
		String preTld = hostnameStr.substring(0, hostnameStr.length() - ".".length() - tldStr.length());
		if(preTld.isEmpty()) throw new IllegalArgumentException("Empty preTld: " + preTld);
		return preTld;
	}

	/* Unused 2018-12-02:
	public boolean addDNSRecord(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		DomainName hostname,
		InetAddress ipAddress,
		List<DomainName> tlds
	) throws IOException, SQLException {
		DomainName tld = ZoneTable.getHostTLD(hostname, tlds);
		String zone = tld + ".";
		boolean exists = conn.executeBooleanQuery(
			"select (select zone from dns.\"Zone\" where zone=?) is not null",
			zone
		);
		if (exists) {
			String preTld = getPreTld(hostname, tld);
			exists = conn.executeBooleanQuery(
				"select (select id from dns.\"Record\" where \"zone\"=? and \"type\"='A' and \"domain\"=?) is not null",
				zone,
				preTld
			);
			if (!exists) {
				String aType;
				switch(ipAddress.getAddressFamily()) {
					case INET :
						aType = RecordType.A;
						break;
					case INET6 :
						aType = RecordType.AAAA;
						break;
					default :
						throw new AssertionError();
				}
				conn.executeUpdate(
					"insert into dns.\"Record\" (\"zone\", \"domain\", \"type\", destination) values (?,?,?,?)",
					zone,
					preTld,
					aType,
					ipAddress
				);
				invalidateList.addTable(
					conn,
					Table.TableID.DNS_RECORDS,
					getBusinessForDNSZone(conn, zone),
					getDNSAOServers(conn),
					false
				);
				updateDNSZoneSerial(conn, invalidateList, zone);
				return true;
			}
		}
		return false;
	}
	 */

	private static void checkAccessDNSRecord(DatabaseConnection conn, RequestSource source, String action, int id) throws IOException, SQLException {
		if(
			!isDNSAdmin(conn, source)
			&& !PackageHandler.canAccessPackage(conn, source, getPackageForDNSRecord(conn, id))
		) {
			String message=
				"business_administrator.username="
				+source.getUsername()
				+" is not allowed to access dns_record: action='"
				+action
				+", id="
				+id
			;
			throw new SQLException(message);
		}
	}

	public boolean canAccessDNSZone(DatabaseConnection conn, RequestSource source, String zone) throws IOException, SQLException {
		return
			isDNSAdmin(conn, source)
			|| PackageHandler.canAccessPackage(conn, source, getPackageForDNSZone(conn, zone))
		;
	}

	private void checkAccessDNSZone(DatabaseConnection conn, RequestSource source, String action, String zone) throws IOException, SQLException {
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
	private static boolean isDNSAdmin(
		DatabaseConnection conn,
		RequestSource source
	) throws IOException, SQLException {
		User mu=MasterServer.getUser(conn, source.getUsername());
		return mu!=null && mu.isDNSAdmin();
	}

	/* Unused 2018-12-02:
	public AccountingCode getBusinessForDNSRecord(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select pk.accounting from dns.\"Record\" nr, dns.\"Zone\" nz, billing.\"Package\" pk where nr.\"zone\"=nz.\"zone\" and nz.package=pk.\"name\" and nr.id=?",
			id
		);
	}
	 */

	private static AccountingCode getBusinessForDNSZone(DatabaseConnection conn, String zone) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select pk.accounting from dns.\"Zone\" nz, billing.\"Package\" pk where nz.package=pk.name and nz.zone=?",
			zone
		);
	}

	private static IntList getDNSAOServers(DatabaseConnection conn) throws IOException, SQLException {
		return conn.executeIntListQuery("select distinct server from net.\"Bind\" where app_protocol=? and server in (select server from linux.\"Server\")", AppProtocol.DNS);
	}

	private static final Object dnstldLock=new Object();
	private static List<DomainName> dnstldCache;
	public List<DomainName> getDNSTLDs(DatabaseConnection conn) throws IOException, SQLException {
		synchronized(dnstldLock) {
			if(dnstldCache==null) {
				dnstldCache=conn.executeObjectCollectionQuery(
					new ArrayList<DomainName>(),
					ObjectFactories.domainNameFactory,
					"select domain from dns.\"TopLevelDomain\""
				);
			}
			return dnstldCache;
		}
	}

	private static String getDNSZoneForDNSRecord(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeStringQuery("select \"zone\" from dns.\"Record\" where id=?", id);
	}

	public boolean isDNSZoneAvailable(DatabaseConnection conn, String zone) throws IOException, SQLException {
		return conn.executeBooleanQuery("select (select zone from dns.\"Zone\" where zone=?) is null", zone);
	}

	private static AccountingCode getPackageForDNSRecord(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select nz.package from dns.\"Record\" nr, dns.\"Zone\" nz where nr.id=? and nr.\"zone\"=nz.\"zone\"",
			id
		);
	}

	private static AccountingCode getPackageForDNSZone(DatabaseConnection conn, String zone) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select package from dns.\"Zone\" where zone=?",
			zone
		);
	}

	public void invalidateTable(Table.TableID tableID) {
		switch(tableID) {
			case DNS_TLDS :
				synchronized(dnstldLock) {
					dnstldCache=null;
				}
				break;
		}
	}

	// TODO: Manage SPF records here
	public void removeUnusedDNSRecord(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		DomainName hostname,
		List<DomainName> tlds
	) throws IOException, SQLException {
		if(conn.executeBooleanQuery("select (select id from web.\"VirtualHostName\" where hostname=? limit 1) is null", hostname)) {
			DomainName tld = ZoneTable.getHostTLD(hostname, tlds);
			String zone = tld + ".";
			if(conn.executeBooleanQuery("select (select zone from dns.\"Zone\" where zone=?) is not null", zone)) {
				String preTld = getPreTld(hostname, tld);
				int deleteCount = conn.executeUpdate(
					"delete from dns.\"Record\" where\n"
					+ "  \"zone\"=?\n"
					+ "  and \"type\" in (?,?)\n"
					+ "  and \"domain\"=?",
					zone,
					RecordType.A, RecordType.AAAA,
					preTld
				);
				if(deleteCount > 0) {
					invalidateList.addTable(
						conn,
						Table.TableID.DNS_RECORDS,
						getBusinessForDNSZone(conn, zone),
						getDNSAOServers(conn),
						false
					);
					updateDNSZoneSerial(conn, invalidateList, zone);
				}
			}
		}
	}

	/**
	 * Sets the default TTL for a <code>Zone</code>.
	 */
	public void setDNSZoneTTL(
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
		conn.executeUpdate("update dns.\"Zone\" set ttl=? where zone=?", ttl, zone);
		invalidateList.addTable(
			conn,
			Table.TableID.DNS_ZONES,
			getBusinessForDNSZone(conn, zone),
			getDNSAOServers(conn),
			false
		);
		updateDNSZoneSerial(conn, invalidateList, zone);
	}

	public void updateDhcpDnsRecords(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int ipAddress,
		InetAddress dhcpAddress
	) throws IOException, SQLException {
		// Find the ids of the entries that should be changed
		IntList ids=conn.executeIntListQuery("select id from dns.\"Record\" where \"dhcpAddress\"=?", ipAddress);

		// Build a list of affected zones
		List<String> zones=new SortedArrayList<>();

		for(int c=0;c<ids.size();c++) {
			int id=ids.getInt(c);
			String zone=getDNSZoneForDNSRecord(conn, id);
			if(!zones.contains(zone)) zones.add(zone);
			conn.executeUpdate("update dns.\"Record\" set destination=? where id=?", dhcpAddress, id);
		}

		// Invalidate the records
		invalidateList.addTable(
			conn,
			Table.TableID.DNS_RECORDS,
			InvalidateList.allBusinesses,
			InvalidateList.allServers,
			false
		);

		// Update the zone serials
		for (String zone : zones) {
			updateDNSZoneSerial(
				conn,
				invalidateList,
				zone
			);
		}
	}

	private static void updateDNSZoneSerial(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		String zone
	) throws IOException, SQLException {
		// Get the old serial
		long serial=conn.executeLongQuery("select serial from dns.\"Zone\" where zone=?", zone);

		// Check if already today or higher
		long todaySerial=Zone.getCurrentSerial();
		if(serial>=todaySerial) {
			// If so, just increment by one
			serial++;
		} else {
			// Otherwise, set it to today with daily of 01
			serial=todaySerial;
		}

		// Place the serial back in the database
		conn.executeUpdate("update dns.\"Zone\" set serial=? where zone=?", serial, zone);
		invalidateList.addTable(
			conn,
			Table.TableID.DNS_ZONES,
			InvalidateList.allBusinesses,
			InvalidateList.allServers,
			false
		);
	}

	public void updateReverseDnsIfExists(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		InetAddress ip,
		DomainName hostname
	) throws IOException, SQLException {
		switch(ip.getAddressFamily()) {
			case INET : {
				final String netmask;
				final String ipStr = ip.toString();
				if(
					ipStr.startsWith("66.160.183.")
					|| ipStr.startsWith("64.62.174.")
				) {
					netmask = "255.255.255.0";
				} else if(ipStr.startsWith("64.71.144.")) {
					netmask = "255.255.255.128";
				} else {
					netmask = null;
				}
				if(netmask!=null) {
					String arpaZone=Zone.getArpaZoneForIPAddress(ip, netmask);
					if(
						conn.executeBooleanQuery(
							"select (select zone from dns.\"Zone\" where zone=?) is not null",
							arpaZone
						)
					) {
						int pos=ipStr.lastIndexOf('.');
						String oct4=ipStr.substring(pos+1);
						if(
							conn.executeBooleanQuery(
								"select (select id from dns.\"Record\" where \"zone\"=? and \"domain\"=? and \"type\"=? limit 1) is not null",
								arpaZone,
								oct4,
								RecordType.PTR
							)
						) {
							updateDNSZoneSerial(conn, invalidateList, arpaZone);

							conn.executeUpdate(
								"update dns.\"Record\" set destination=? where \"zone\"=? and \"domain\"=? and \"type\"=?",
								hostname.toString()+'.',
								arpaZone,
								oct4,
								RecordType.PTR
							);
							invalidateList.addTable(conn, Table.TableID.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
						}
					}
				}
				break;
			}
			case INET6 :
				throw new NotImplementedException();
			default :
				throw new AssertionError();
		}
	}
}
