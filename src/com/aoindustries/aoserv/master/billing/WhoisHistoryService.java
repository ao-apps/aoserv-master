/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.billing;

import com.aoindustries.aoserv.client.dns.ZoneTable;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.master.BusinessHandler;
import com.aoindustries.aoserv.master.InvalidateList;
import com.aoindustries.aoserv.master.LogFactory;
import com.aoindustries.aoserv.master.MasterDatabase;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.MasterService;
import com.aoindustries.aoserv.master.ObjectFactories;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.dns.DnsService;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.CronJobScheduleMode;
import com.aoindustries.cron.Schedule;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.net.DomainName;
import com.aoindustries.util.logging.ProcessTimer;
import com.aoindustries.validation.ValidationException;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles all the accesses to the whois history.
 *
 * @author  AO Industries, Inc.
 */
final public class WhoisHistoryService implements MasterService, CronJob {

	private static final Logger logger = LogFactory.getLogger(WhoisHistoryService.class);

	/**
	 * The time to sleep between lookups in millis.
	 */
	private static final int LOOKUP_SLEEP = 60 * 1000; // One minute

	/**
	 * The maximum time for a processing pass.
	 */
	private static final long TIMER_MAX_TIME = 20L*60*1000;

	/**
	 * The interval in which the administrators will be reminded.
	 */
	private static final long TIMER_REMINDER_INTERVAL = 6L*60*60*1000;

	/**
	 * The amount of time to keep whois history, used as a PostgreSQL interval.
	 */
	private static final String
		CLEANUP_AFTER_GOOD_ACCOUNT = "7 years", // Was 1 year, is this overkill?
		CLEANUP_AFTER_CLOSED_ACCOUNT_ZERO_BALANCE = "7 years", // Was 1 year, is this overkill?
		CLEANUP_AFTER_CLOSED_ACCOUNT_NO_TRANSACTIONS = "1 year";

	@Override
	public void start() {
		CronDaemon.addCronJob(this, logger);
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
		return getClass().getSimpleName();
	}

	@Override
	public int getCronJobThreadPriority() {
		return Thread.NORM_PRIORITY-1;
	}

	// TODO: Should we fire this off manually, or at least have a way to do so when the process fails?
	// TODO: Should there be a monthly task to make sure this process is working correctly?
	// TODO: Do not run simply as a cron job, but rather a background process that does again based on when
	//       last successful/failed.  This will be more robust to master server restarts and allow to
	//       slowly work in the background.
	// TODO: This should probably go in a dns.monitoring schema, and be watched by NOC monitoring.
	@Override
	public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
		try {
			ProcessTimer timer = new ProcessTimer(
				logger,
				MasterServer.getRandom(),
				getClass().getName(),
				"runCronJob",
				WhoisHistoryService.class.getSimpleName() + " - Whois History",
				"Looking up whois and cleaning old records",
				TIMER_MAX_TIME,
				TIMER_REMINDER_INTERVAL
			);
			try {
				MasterServer.executorService.submit(timer);

				// Start the transaction
				InvalidateList invalidateList = new InvalidateList();
				DatabaseConnection conn = MasterDatabase.getDatabase().createDatabaseConnection();
				try {
					boolean connRolledBack = false;
					try {
						/*
						 * Remove old records first
						 */
						// Open account that have balance <= $0.00 and entry is older than one year
						int updated = conn.executeUpdate(
							"delete from billing.\"WhoisHistory\" where id in (\n"
							+ "  select\n"
							+ "    wh.id\n"
							+ "  from\n"
							+ "               billing.\"WhoisHistory\" wh\n"
							+ "    inner join account.\"Account\"      bu on wh.accounting = bu.accounting\n"
							+ "    left  join billing.account_balances ab on bu.accounting = ab.accounting"
							+ "  where\n"
							// entry is older than interval
							+ "    (now()-wh.time) > ?::interval\n"
							// open account
							+ "    and bu.canceled is null\n"
							// balance is <= $0.00
							+ "    and (ab.accounting is null or ab.balance<='0.00'::decimal(9,2))"
							+ ")",
							CLEANUP_AFTER_GOOD_ACCOUNT
						);
						if(updated > 0) invalidateList.addTable(conn, Table.TableID.WHOIS_HISTORY, InvalidateList.allBusinesses, InvalidateList.allServers, false);

						// Closed account that have a balance of $0.00, has not had any billing.Transaction for interval, and entry is older than interval
						updated = conn.executeUpdate(
							"delete from billing.\"WhoisHistory\" where id in (\n"
							+ "  select\n"
							+ "    wh.id\n"
							+ "  from\n"
							+ "               billing.\"WhoisHistory\" wh\n"
							+ "    inner join account.\"Account\"      bu on wh.accounting = bu.accounting\n"
							+ "    left  join billing.account_balances ab on bu.accounting = ab.accounting"
							+ "  where\n"
							// entry is older than interval
							+ "    (now()-wh.time) > ?::interval\n"
							// closed account
							+ "    and bu.canceled is not null\n"
							// has not had any accounting billing.Transaction for interval
							+ "    and (select tr.transid from billing.\"Transaction\" tr where bu.accounting=tr.accounting and tr.time>=(now() - ?::interval) limit 1) is null\n"
							// balance is $0.00
							+ "    and (ab.accounting is null or ab.balance='0.00'::decimal(9,2))"
							+ ")",
							CLEANUP_AFTER_CLOSED_ACCOUNT_ZERO_BALANCE,
							CLEANUP_AFTER_CLOSED_ACCOUNT_NO_TRANSACTIONS
						);
						if(updated > 0) invalidateList.addTable(conn, Table.TableID.WHOIS_HISTORY, InvalidateList.allBusinesses, InvalidateList.allServers, false);
						conn.commit();
						MasterServer.invalidateTables(invalidateList, null);
						invalidateList.reset();

						/*
						 * The add new records
						 */
						// Get the set of unique accounting, zone combinations in the system
						Set<AccountingAndZone> topLevelZones = getBusinessesAndTopLevelZones(conn);
						conn.releaseConnection();

						// Perform the whois lookups once per unique zone
						Set<String> zonesDone = new HashSet<>(topLevelZones.size()*4/3+1);
						for(AccountingAndZone aaz : topLevelZones) {
							String zone = aaz.getZone();
							if(zonesDone.add(zone)) {
								String whoisOutput;
								try {
									whoisOutput = getWhoisOutput(zone);
								} catch(Throwable err) {
									whoisOutput = err.toString();
								}
								// update database
								// TODO: Store a success flag, too
								// TODO: Store the parsed nameservers, too?  At least for when is success.
								String accounting = aaz.getAccounting();
								conn.executeUpdate(
									"insert into billing.\"WhoisHistory\" (accounting, zone, whois_output) values(?,?,?)",
									accounting,
									zone,
									whoisOutput
								);
								invalidateList.addTable(conn, Table.TableID.WHOIS_HISTORY, InvalidateList.allBusinesses, InvalidateList.allServers, false);
								conn.commit();
								conn.releaseConnection();
								MasterServer.invalidateTables(invalidateList, null);
								invalidateList.reset();
								try {
									Thread.sleep(LOOKUP_SLEEP);
								} catch(InterruptedException e) {
									logger.log(Level.WARNING, null, e);
								}
							}
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
	private static String getWhoisOutput(String zone) throws IOException {
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
	 * @see  ZoneTable#getHostTLD(com.aoindustries.net.DomainName, java.util.List)
	 */
	private Set<AccountingAndZone> getBusinessesAndTopLevelZones(DatabaseConnection conn) throws IOException, SQLException {
		List<DomainName> tlds = MasterServer.getService(DnsService.class).getDNSTLDs(conn);

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
						tld = ZoneTable.getHostTLD(domain, tlds) + ".";
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

	private static AccountingCode getBusinessForWhoisHistory(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select accounting from billing.\"WhoisHistory\" where id=?",
			id
		);
	}

	/**
	 * Gets the whois output for the specific billing.WhoisHistory record.
	 */
	public String getWhoisHistoryOutput(DatabaseConnection conn, RequestSource source, int id) throws IOException, SQLException {
		AccountingCode accounting = getBusinessForWhoisHistory(conn, id);
		BusinessHandler.checkAccessBusiness(conn, source, "getWhoisHistoryOutput", accounting);
		return conn.executeStringQuery("select whois_output from billing.\"WhoisHistory\" where id=?", id);
	}

	private static class AccountingAndZone {

		final private String accounting;
		final private String zone;

		private AccountingAndZone(String accounting, String zone) {
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
