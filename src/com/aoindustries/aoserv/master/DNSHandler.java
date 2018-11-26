/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
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
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.CronJobScheduleMode;
import com.aoindustries.cron.Schedule;
import com.aoindustries.dbc.DatabaseAccess.Null;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.lang.NotImplementedException;
import com.aoindustries.net.DomainName;
import com.aoindustries.net.InetAddress;
import com.aoindustries.util.IntList;
import com.aoindustries.util.SortedArrayList;
import com.aoindustries.util.logging.ProcessTimer;
import com.aoindustries.validation.ValidationException;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.sql.ResultSet;
import java.sql.SQLException;
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

		/**
		 * Runs at 6:12 am on the 1st, 7th, 13th, 19th, and 25th
		 */
	private static final Schedule schedule = (minute, hour, dayOfMonth, month, dayOfWeek, year) ->
		minute==12
		&& hour==6
		&& (
			dayOfMonth==1
				|| dayOfMonth==7
				|| dayOfMonth==13
				|| dayOfMonth==19
				|| dayOfMonth==25
			);

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
							"delete from billing.\"WhoisHistory\" where id in (\n"
							+ "  select\n"
							+ "    wh.id\n"
							+ "  from\n"
							+ "               billing.\"WhoisHistory\" wh\n"
							+ "    inner join account.\"Account\"      bu on wh.accounting = bu.accounting\n"
							+ "    left  join billing.account_balances ab on bu.accounting = ab.accounting"
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

						// Closed account that have a balance of $0.00, has not had any billing.Transaction for one year, and entry is older than one year
						updated = conn.executeUpdate(
							"delete from billing.\"WhoisHistory\" where id in (\n"
							+ "  select\n"
							+ "    wh.id\n"
							+ "  from\n"
							+ "               billing.\"WhoisHistory\" wh\n"
							+ "    inner join account.\"Account\"      bu on wh.accounting = bu.accounting\n"
							+ "    left  join billing.account_balances ab on bu.accounting = ab.accounting"
							+ "  where\n"
							// entry is older than one year
							+ "    (now()-wh.time)>'1 year'::interval\n"
							// closed account
							+ "    and bu.canceled is not null\n"
							// has not had any accounting billing.Transaction for one year
							+ "    and (select tr.transid from billing.\"Transaction\" tr where bu.accounting=tr.accounting and tr.time>=(now()-'1 year'::interval) limit 1) is null\n"
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
							conn.executeUpdate("insert into billing.\"WhoisHistory\" (accounting, zone, whois_output) values(?,?,?)", accounting, zone, whoisOutput);
							invalidateList.addTable(conn, SchemaTable.TableID.WHOIS_HISTORY, InvalidateList.allBusinesses, InvalidateList.allServers, false);
						}
					} catch(RuntimeException | IOException err) {
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
				MasterServer.invalidateTables(invalidateList, null);
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
	 * @see  DNSZoneTable#getHostTLD(com.aoindustries.net.DomainName, java.util.List)
	 */
	public static Set<AccountingAndZone> getBusinessesAndTopLevelZones(DatabaseConnection conn) throws IOException, SQLException {
		List<DomainName> tlds = getDNSTLDs(conn);

		return conn.executeQuery(
			(ResultSet results) -> {
				Set<AccountingAndZone> aazs = new HashSet<>();
				while(results.next()) {
					String accounting = results.getString(1);
					String zone = results.getString(2);
					if(!zone.endsWith(".")) throw new SQLException("No end '.': " + zone);
					DomainName domain;
					try {
						domain = DomainName.valueOf(zone.substring(0, zone.length() - 1));
					} catch(ValidationException e) {
						throw new SQLException(e);
					}
					String tld;
					try {
						tld = DNSZoneTable.getHostTLD(domain, tlds) + ".";
					} catch(IllegalArgumentException err) {
						logger.log(Level.WARNING, null, err);
						tld = zone;
					}
					AccountingAndZone aaz = new AccountingAndZone(accounting, tld);
					if(!aazs.contains(aaz)) aazs.add(aaz);
				}
				return aazs;
			},
			"select distinct\n"
			+ "  pk.accounting as accounting,\n"
			+ "  dz.zone as zone\n"
			+ "from\n"
			+ "  dns.\"Zone\" dz\n"
			+ "  inner join billing.\"Package\" pk on dz.package=pk.name\n"
			+ "where\n"
			+ "  dz.zone not like '%.in-addr.arpa'\n"
			+ "union select distinct\n"
			+ "  pk.accounting as accounting,\n"
			+ "  ed.domain||'.' as zone\n"
			+ "from\n"
			+ "  email.\"Domain\" ed\n"
			+ "  inner join billing.\"Package\" pk on ed.package=pk.name\n"
			+ "union select distinct\n"
			+ "  pk.accounting as accounting,\n"
			+ "  hsu.hostname||'.' as zone\n"
			+ "from\n"
			+ "  web.\"VirtualHostName\" hsu\n"
			+ "  inner join web.\"VirtualHost\" hsb on hsu.httpd_site_bind=hsb.id\n"
			+ "  inner join web.\"Site\" hs on hsb.httpd_site=hs.id\n"
			+ "  inner join billing.\"Package\" pk on hs.package=pk.name\n"
			+ "  inner join linux.\"Server\" ao on hs.ao_server=ao.server\n"
			+ "where\n"
			// Is not "localhost"
			+ "  hsu.hostname!='localhost'\n"
			// Is not the test URL
			+ "  and hsu.hostname!=(hs.\"name\" || '.' || ao.hostname)"
		);
	}

	/**
	 * Gets the whois output for the specific billing.WhoisHistory record.
	 */
	public static String getWhoisHistoryOutput(DatabaseConnection conn, RequestSource source, int id) throws IOException, SQLException {
		AccountingCode accounting = getBusinessForWhoisHistory(conn, id);
		BusinessHandler.checkAccessBusiness(conn, source, "getWhoisHistoryOutput", accounting);
		return conn.executeStringQuery("select whois_output from billing.\"WhoisHistory\" where id=?", id);
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
			if(priority == DNSRecord.NO_PRIORITY) throw new IllegalArgumentException("priority required for type=" + type);
			else if(priority<=0) throw new SQLException("Invalid priority: " + priority);
		} else {
			if(priority != DNSRecord.NO_PRIORITY) throw new SQLException("No priority allowed for type="+type);
		}

		// Must have appropriate weight
		if(conn.executeBooleanQuery("select has_weight from dns.\"RecordType\" where type=?", type)) {
			if(weight == DNSRecord.NO_WEIGHT) throw new IllegalArgumentException("weight required for type=" + type);
			else if(weight<=0) throw new SQLException("Invalid weight: " + weight);
		} else {
			if(weight != DNSRecord.NO_WEIGHT) throw new SQLException("No weight allowed for type="+type);
		}

		// Must have appropriate port
		if(conn.executeBooleanQuery("select has_port from dns.\"RecordType\" where type=?", type)) {
			if(port == DNSRecord.NO_PORT) throw new IllegalArgumentException("port required for type=" + type);
			else if(port < 1 || port > 65535) throw new SQLException("Invalid port: " + port);
		} else {
			if(port != DNSRecord.NO_PORT) throw new SQLException("No port allowed for type="+type);
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

		// Add the entry
		int id = conn.executeIntUpdate(
			"INSERT INTO dns.\"Record\" VALUES (default,?,?,?,?,?,?,null,?) RETURNING id",
			zone,
			domain,
			type,
			(priority == DNSRecord.NO_PRIORITY) ? Null.INTEGER : priority,
			(weight == DNSRecord.NO_WEIGHT) ? Null.INTEGER : weight,
			(port == DNSRecord.NO_PORT) ? Null.INTEGER : port,
			destination,
			(ttl == -1) ? Null.INTEGER : ttl
		);
		invalidateList.addTable(conn, SchemaTable.TableID.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);

		// Update the serial of the zone
		updateDNSZoneSerial(conn, invalidateList, zone);

		// Notify all clients of the update
		return id;
	}

	/**
	 * Creates a new <code>DNSZone</code>.
	 */
	public static void addDNSZone(
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
		if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Not allowed to add DNSZone to disabled Package: "+packageName);
		MasterServer.checkAccessHostname(conn, source, "addDNSZone", zone);
		// Check the zone format
		List<DomainName> tlds=getDNSTLDs(conn);
		if(!DNSZoneTable.checkDNSZone(zone, tlds)) throw new SQLException("Invalid zone: "+zone);

		// Must not be allocated in any way to another account
		MasterServer.checkAccessHostname(conn, source, "addDNSZone", zone);

		// Add the dns_zone entry
		conn.executeUpdate(
			"insert into dns.\"Zone\" values(?,?,?,?,?,?)",
			zone,
			zone,
			packageName,
			DNSZone.DEFAULT_HOSTMASTER,
			DNSZone.getCurrentSerial(),
			ttl
		);

		// Add the MX entry
		conn.executeUpdate(
			"insert into dns.\"Record\"(zone, domain, type, priority, destination) values(?,?,?,?,?)",
			zone,
			"@",
			DNSType.MX,
			DNSZone.DEFAULT_MX_PRIORITY,
			"mail"
		);

		final String INSERT_RECORD = "insert into dns.\"Record\"(zone, domain, type, destination) values(?,?,?,?)";
		// TODO: Take a "mail exchanger" parameter to properly setup the default MX records.
		//       If in this domain, sets up SPF like below.  If outside this domain (ends in .),
		//       sets up MX to the mail exchanger, and CNAME "mail" to the mail exchanger.

		// TODO: Take nameservers from reseller.Brand

		String aType;
		switch(ip.getAddressFamily()) {
			case INET :
				aType = DNSType.A;
				break;
			case INET6 :
				aType = DNSType.AAAA;
				break;
			default :
				throw new AssertionError();
		}

		conn.executeUpdate(INSERT_RECORD, zone, "@",    DNSType.NS,  "ns1.aoindustries.com.");
		conn.executeUpdate(INSERT_RECORD, zone, "@",    DNSType.NS,  "ns2.aoindustries.com.");
		conn.executeUpdate(INSERT_RECORD, zone, "@",    DNSType.NS,  "ns3.aoindustries.com.");
		conn.executeUpdate(INSERT_RECORD, zone, "@",    DNSType.NS,  "ns4.aoindustries.com.");
		conn.executeUpdate(INSERT_RECORD, zone, "@",    DNSType.TXT, "v=spf1 a mx -all");
		conn.executeUpdate(INSERT_RECORD, zone, "@",    aType,       ip.toString());
		/*
		conn.executeUpdate(INSERT_RECORD, zone, "ftp",  aType,       ip.toString());
		conn.executeUpdate(INSERT_RECORD, zone, "ftp",  DNSType.TXT, "v=spf1 -all");
		 */
		conn.executeUpdate(INSERT_RECORD, zone, "mail", aType,       ip.toString());
		// See http://www.openspf.org/FAQ/Common_mistakes#helo "Publish SPF records for HELO names used by your mail servers"
		conn.executeUpdate(INSERT_RECORD, zone, "mail", DNSType.TXT, "v=spf1 a -all");
		conn.executeUpdate(INSERT_RECORD, zone, "www",  aType,       ip.toString());
		// See http://www.openspf.org/FAQ/Common_mistakes#all-domains "Publish null SPF records for your domains that don't send mail"
		conn.executeUpdate(INSERT_RECORD, zone, "www",  DNSType.TXT, "v=spf1 -all");

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
		int id
	) throws IOException, SQLException {
		// Must be allowed to access this zone record
		checkAccessDNSRecord(conn, source, "removeDNSRecord", id);

		// Get the zone associated with the id
		String zone=getDNSZoneForDNSRecord(conn, id);

		// Remove the dns.Record entry
		conn.executeUpdate("delete from dns.\"Record\" where id=?", id);
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
		// Remove the dns.Record entries
		conn.executeUpdate("delete from dns.\"Record\" where zone=?", zone);

		// Remove the dns.Zone entry
		conn.executeUpdate("delete from dns.\"Zone\" where zone=?", zone);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
		invalidateList.addTable(conn, SchemaTable.TableID.DNS_ZONES, InvalidateList.allBusinesses, InvalidateList.allServers, false);
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

	public static boolean addDNSRecord(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		DomainName hostname,
		InetAddress ipAddress,
		List<DomainName> tlds
	) throws IOException, SQLException {
		DomainName tld = DNSZoneTable.getHostTLD(hostname, tlds);
		String zone = tld + ".";
		boolean exists = conn.executeBooleanQuery(
			"select (select zone from dns.\"Zone\" where zone=?) is not null",
			zone
		);
		if (exists) {
			String preTld = getPreTld(hostname, tld);
			exists = conn.executeBooleanQuery(
				"select (select id from dns.\"Record\" where zone=? and type='A' and domain=?) is not null",
				zone,
				preTld
			);
			if (!exists) {
				String aType;
				switch(ipAddress.getAddressFamily()) {
					case INET :
						aType = DNSType.A;
						break;
					case INET6 :
						aType = DNSType.AAAA;
						break;
					default :
						throw new AssertionError();
				}
				conn.executeUpdate(
					"insert into dns.\"Record\" (zone, domain, type, destination) values (?,?,?,?)",
					zone,
					preTld,
					aType,
					ipAddress
				);
				invalidateList.addTable(
					conn,
					SchemaTable.TableID.DNS_RECORDS,
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

	public static void checkAccessDNSRecord(DatabaseConnection conn, RequestSource source, String action, int id) throws IOException, SQLException {
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

	public static AccountingCode getBusinessForDNSRecord(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select pk.accounting from dns.\"Record\" nr, dns.\"Zone\" nz, billing.\"Package\" pk where nr.zone=nz.zone and nz.package=pk.name and nr.id=?",
			id
		);
	}

	public static AccountingCode getBusinessForDNSZone(DatabaseConnection conn, String zone) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select pk.accounting from dns.\"Zone\" nz, billing.\"Package\" pk where nz.package=pk.name and nz.zone=?",
			zone
		);
	}

	public static AccountingCode getBusinessForWhoisHistory(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select accounting from billing.\"WhoisHistory\" where id=?",
			id
		);
	}

	public static IntList getDNSAOServers(DatabaseConnection conn) throws IOException, SQLException {
		return conn.executeIntListQuery("select distinct server from net.\"Bind\" where app_protocol=? and server in (select server from linux.\"Server\")", Protocol.DNS);
	}

	private static final Object dnstldLock=new Object();
	private static List<DomainName> dnstldCache;
	public static List<DomainName> getDNSTLDs(DatabaseConnection conn) throws IOException, SQLException {
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

	public static String getDNSZoneForDNSRecord(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeStringQuery("select zone from dns.\"Record\" where id=?", id);
	}

	public static boolean isDNSZoneAvailable(DatabaseConnection conn, String zone) throws IOException, SQLException {
		return conn.executeBooleanQuery("select (select zone from dns.\"Zone\" where zone=?) is null", zone);
	}

	public static AccountingCode getPackageForDNSRecord(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select nz.package from dns.\"Record\" nr, dns.\"Zone\" nz where nr.id=? and nr.zone=nz.zone",
			id
		);
	}

	public static AccountingCode getPackageForDNSZone(DatabaseConnection conn, String zone) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select package from dns.\"Zone\" where zone=?",
			zone
		);
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

	// TODO: Manage SPF records here
	public static void removeUnusedDNSRecord(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		DomainName hostname,
		List<DomainName> tlds
	) throws IOException, SQLException {
		if(conn.executeBooleanQuery("select (select id from web.\"VirtualHostName\" where hostname=? limit 1) is null", hostname)) {
			DomainName tld = DNSZoneTable.getHostTLD(hostname, tlds);
			String zone = tld + ".";
			if(conn.executeBooleanQuery("select (select zone from dns.\"Zone\" where zone=?) is not null", zone)) {
				String preTld = getPreTld(hostname, tld);
				int deleteCount = conn.executeUpdate(
					"delete from dns.\"Record\" where\n"
					+ "  zone=?\n"
					+ "  and type in (?,?)\n"
					+ "  and domain=?",
					zone,
					DNSType.A, DNSType.AAAA,
					preTld
				);
				if(deleteCount > 0) {
					invalidateList.addTable(
						conn,
						SchemaTable.TableID.DNS_RECORDS,
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
		conn.executeUpdate("update dns.\"Zone\" set ttl=? where zone=?", ttl, zone);
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
			SchemaTable.TableID.DNS_RECORDS,
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

	public static void updateDNSZoneSerial(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		String zone
	) throws IOException, SQLException {
		// Get the old serial
		long serial=conn.executeLongQuery("select serial from dns.\"Zone\" where zone=?", zone);

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
		conn.executeUpdate("update dns.\"Zone\" set serial=? where zone=?", serial, zone);
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
					String arpaZone=DNSZone.getArpaZoneForIPAddress(ip, netmask);
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
								"select (select id from dns.\"Record\" where zone=? and domain=? and type=? limit 1) is not null",
								arpaZone,
								oct4,
								DNSType.PTR
							)
						) {
							updateDNSZoneSerial(conn, invalidateList, arpaZone);

							conn.executeUpdate(
								"update dns.\"Record\" set destination=? where zone=? and domain=? and type=?",
								hostname.toString()+'.',
								arpaZone,
								oct4,
								DNSType.PTR
							);
							invalidateList.addTable(conn, SchemaTable.TableID.DNS_RECORDS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
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
