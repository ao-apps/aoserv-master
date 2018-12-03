/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.billing;

import com.aoindustries.aoserv.client.billing.WhoisHistory;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.master.BusinessHandler;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.InvalidateList;
import com.aoindustries.aoserv.master.LogFactory;
import com.aoindustries.aoserv.master.MasterDatabase;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.MasterService;
import com.aoindustries.aoserv.master.ObjectFactories;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.CronJobScheduleMode;
import com.aoindustries.cron.Schedule;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.lang.ProcessResult;
import com.aoindustries.net.DomainName;
import com.aoindustries.util.logging.ProcessTimer;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles all the accesses to the whois history.
 *
 * @author  AO Industries, Inc.
 */
// TODO: Also do whois history for:
//       SmtpSmartHost?
//       Server.hostname?
//       CyrusImapdBind.servername
//       CyrusImapdServer.servername
//       SendmailServer
//       ftp.PrivateServer.hostname
//       IpAddress.hostname
final public class WhoisHistoryService implements MasterService {

	private static final Logger logger = LogFactory.getLogger(WhoisHistoryService.class);

	private static final boolean DEBUG = false;

	@Override
	public void start() {
		CronDaemon.addCronJob(cronJob, logger);
		// Run at start-up, too
		CronDaemon.runImmediately(cronJob);
	}

	// <editor-fold desc="Clean-up" defaultstate="collapsed">
	/**
	 * The amount of time to keep whois history, used as a PostgreSQL interval.
	 */
	private static final String
		CLEANUP_AFTER_GOOD_ACCOUNT = "7 years", // Was 1 year, is this overkill?
		CLEANUP_AFTER_CLOSED_ACCOUNT_ZERO_BALANCE = "7 years", // Was 1 year, is this overkill?
		CLEANUP_AFTER_CLOSED_ACCOUNT_NO_TRANSACTIONS = "1 year";

	private static void cleanup(DatabaseConnection conn, InvalidateList invalidateList) throws IOException, SQLException {
		Set<AccountingCode> accountsAffected = new HashSet<>();

		// Open account that have balance <= $0.00 and entry is older than one year
		List<AccountingCode> deletedGoodStanding = conn.executeObjectListUpdate(
			ObjectFactories.accountingCodeFactory,
			"DELETE FROM billing.\"WhoisHistory\" WHERE id IN (\n"
			+ "  SELECT\n"
			+ "    wh.id\n"
			+ "  FROM\n"
			+ "               billing.\"WhoisHistory\" wh\n"
			+ "    INNER JOIN account.\"Account\"      bu ON wh.accounting = bu.accounting\n"
			+ "    LEFT  JOIN billing.account_balances ab ON bu.accounting = ab.accounting"
			+ "  WHERE\n"
			// entry is older than interval
			+ "    (now() - wh.time) > ?::interval\n"
			// open account
			+ "    AND bu.canceled IS NULL\n"
			// balance is <= $0.00
			+ "    AND (ab.accounting IS NULL OR ab.balance <= '0.00'::numeric(9,2))"
			+ ") RETURNING accounting",
			CLEANUP_AFTER_GOOD_ACCOUNT
		);
		if(!deletedGoodStanding.isEmpty()) {
			accountsAffected.addAll(deletedGoodStanding);
			if(DEBUG) System.out.println(WhoisHistoryService.class.getSimpleName() + ": cleanup: Deleted good standing: " + deletedGoodStanding.size());
		}

		// Closed account that have a balance of $0.00, has not had any billing.Transaction for interval, and entry is older than interval
		List<AccountingCode> deletedCanceledZero = conn.executeObjectListUpdate(
			ObjectFactories.accountingCodeFactory,
			"DELETE FROM billing.\"WhoisHistory\" WHERE id IN (\n"
			+ "  SELECT\n"
			+ "    wh.id\n"
			+ "  FROM\n"
			+ "               billing.\"WhoisHistory\" wh\n"
			+ "    INNER JOIN account.\"Account\"      bu ON wh.accounting = bu.accounting\n"
			+ "    LEFT  JOIN billing.account_balances ab ON bu.accounting = ab.accounting"
			+ "  WHERE\n"
			// entry is older than interval
			+ "    (now() - wh.time) > ?::interval\n"
			// closed account
			+ "    AND bu.canceled IS NOT NULL\n"
			// has not had any accounting billing.Transaction for interval
			+ "    AND (SELECT tr.transid FROM billing.\"Transaction\" tr WHERE bu.accounting = tr.accounting AND tr.\"time\" >= (now() - ?::interval) LIMIT 1) IS NULL\n"
			// balance is $0.00
			+ "    AND (ab.accounting IS NULL OR ab.balance = '0.00'::numeric(9,2))"
			+ ") RETURNING accounting",
			CLEANUP_AFTER_CLOSED_ACCOUNT_ZERO_BALANCE,
			CLEANUP_AFTER_CLOSED_ACCOUNT_NO_TRANSACTIONS
		);
		if(!deletedCanceledZero.isEmpty()) {
			accountsAffected.addAll(deletedCanceledZero);
			if(DEBUG) System.out.println(WhoisHistoryService.class.getSimpleName() + ": cleanup: Deleted canceled at zero balance: " + deletedCanceledZero.size());
		}
		if(!accountsAffected.isEmpty()) {
			invalidateList.addTable(
				conn,
				Table.TableID.WHOIS_HISTORY,
				accountsAffected,
				InvalidateList.allServers,
				false
			);
		}
	}
	// </editor-fold>

	// <editor-fold desc="CronJob" defaultstate="collapsed">
	/**
	 * The minimum time to sleep between lookups in millis.
	 */
	private static final int LOOKUP_SLEEP_MINIMUM = 10 * 1000; // 10 seconds

	/**
	 * The interval between checks.
	 */
	private static final long RECHECK_MILLIS = 6L * 24 * 60 * 60 * 1000; // 6 days

	/**
	 * The target time for processing pass completion.
	 */
	private static final long PASS_COMPLETION_TARGET = RECHECK_MILLIS / 2; // Half the recheck time

	/**
	 * The maximum time for a processing pass.
	 */
	private static final long TIMER_MAX_TIME = RECHECK_MILLIS;

	/**
	 * The interval in which the administrators will be reminded.
	 */
	private static final long TIMER_REMINDER_INTERVAL = 24L*60*60*1000; // 1 day

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

	/**
	 * When last run failed, runs hourly at HH:12
	 */
	private static final Schedule failedSchedule = (minute, hour, dayOfMonth, month, dayOfWeek, year) ->
		minute==12;

	private final CronJob cronJob = new CronJob() {

		private volatile boolean lastRunSuccessful = false;

		@Override
		public Schedule getCronJobSchedule() {
			return lastRunSuccessful ? schedule : failedSchedule;
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
		//       last successful/failed.  This would be less rigidly scheduled.
		// TODO: This should probably go in a dns.monitoring schema, and be watched by NOC monitoring.
		@Override
		public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
			lastRunSuccessful = false;
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
							cleanup(conn, invalidateList);
							conn.commit();
							MasterServer.invalidateTables(invalidateList, null);
							invalidateList.reset();

							/*
							 * The add new records
							 */
							// Get the set of unique registrable domains and accounts in the system
							Map<DomainName,Set<AccountingCode>> registrableDomains = getWhoisHistoryDomains(conn);
							conn.releaseConnection();

							// Find the number of distinct registrable domains
							int registrableDomainCount = registrableDomains.size();
							if(registrableDomainCount > 0) {
								// Compute target sleep time
								final long targetSleepTime = PASS_COMPLETION_TARGET / registrableDomainCount;
								if(DEBUG) {
									System.out.println(
										WhoisHistoryService.class.getSimpleName()
										+ ": Target sleep time for "
										+ registrableDomainCount
										+ " registrable "
										+ (registrableDomainCount==1 ? "domain" : "domains")
										+ " is " + targetSleepTime + " ms"
									);
								}
								
								// Performs the whois lookup once per unique registrable domain
								for(Map.Entry<DomainName,Set<AccountingCode>> entry : registrableDomains.entrySet()) {
									final DomainName registrableDomain = entry.getKey();
									final Set<AccountingCode> accounts = entry.getValue();
									final int numAccounts = accounts.size();
									// Lookup the last time this registrable domain was done for each account
									final Map<AccountingCode,Timestamp> lastChecked;
									{
										StringBuilder sql = new StringBuilder();
										sql.append("SELECT\n"
											+ "  a.accounting,\n"
											+ "  (SELECT wh.\"time\" FROM billing.\"WhoisHistory\" wh WHERE a.accounting=wh.accounting AND \"zone\"=? ORDER BY \"time\" DESC LIMIT 1) AS \"time\"\n"
											+ "FROM\n"
											+ "  account.\"Account\" a\n"
											+ "WHERE\n"
											+ "  a.accounting IN (");
										for(int i = 0; i < numAccounts; i++) {
											if(i != 0) sql.append(',');
											sql.append('?');
										}
										sql.append(')');
										List<Object> params = new ArrayList<>(1 + numAccounts);
										params.add(registrableDomain + "."); // TODO: No "." once column type changed
										params.addAll(accounts);
										lastChecked = conn.executeQuery(
											(ResultSet results) -> {
												try {
													Map<AccountingCode, Timestamp> map = new HashMap<>(numAccounts*4/3+1);
													while(results.next()) {
														map.put(
															AccountingCode.valueOf(results.getString(1)),
															results.getTimestamp(2)
														);
													}
													return map;
												} catch(ValidationException e) {
													throw new SQLException(e);
												}
											},
											sql.toString(),
											params.toArray()
										);
									}
									// Find all accounts that were not checked recently, avoid doing again
									Set<AccountingCode> accountsToCheck = new HashSet<>(numAccounts*4/3+1);
									for(AccountingCode account : accounts) {
										Timestamp time = lastChecked.get(account);
										boolean checkNow;
										if(time == null) {
											checkNow = true;
											if(DEBUG) System.out.println(WhoisHistoryService.class.getSimpleName() + ": " + registrableDomain + ": Never checked for " + account + ", checkNow: " + checkNow);
										} else {
											long timeSince = System.currentTimeMillis() - time.getTime();
											checkNow = timeSince >= RECHECK_MILLIS || timeSince <= -RECHECK_MILLIS;
											if(DEBUG) System.out.println(WhoisHistoryService.class.getSimpleName() + ": " + registrableDomain + ": Last checked " + time + " for " + account + ", checkNow: " + checkNow);
										}
										if(checkNow) accountsToCheck.add(account);
									}
									if(!accountsToCheck.isEmpty()) {
										long startTime = System.currentTimeMillis();
										String whoisOutput;
										try {
											whoisOutput = getWhoisOutput(registrableDomain);
											if(DEBUG) System.out.println(WhoisHistoryService.class.getSimpleName() + ": " + registrableDomain + ": Success");
										} catch(Throwable err) {
											whoisOutput = err.toString();
											if(DEBUG) System.out.println(WhoisHistoryService.class.getSimpleName() + ": " + registrableDomain + ": Error");
										}
										// update database
										// TODO: Store a success flag, too?
										// TODO: Store the parsed nameservers, too?  At least for when is success.
										// This could be a batch, but this is short and simple
										for(AccountingCode account : accountsToCheck) {
											conn.executeUpdate(
												"insert into billing.\"WhoisHistory\" (accounting, \"zone\", whois_output) values(?,?,?)",
												account,
												registrableDomain + ".", // TODO: Do not add "." once column type changes
												whoisOutput
											);
										}
										invalidateList.addTable(
											conn,
											Table.TableID.WHOIS_HISTORY,
											accountsToCheck,
											InvalidateList.allServers,
											false
										);
										conn.commit();
										conn.releaseConnection();
										MasterServer.invalidateTables(invalidateList, null);
										invalidateList.reset();
										try {
											long sleepTime = targetSleepTime - (System.currentTimeMillis() - startTime);
											if(sleepTime < LOOKUP_SLEEP_MINIMUM) {
												sleepTime = LOOKUP_SLEEP_MINIMUM;
											}
											if(DEBUG) System.out.println(WhoisHistoryService.class.getSimpleName() + ": " + registrableDomain + ": Completed, sleeping " + sleepTime + " ms");
											Thread.sleep(sleepTime);
										} catch(InterruptedException e) {
											logger.log(Level.WARNING, null, e);
										}
									}
								}
							} else {
								if(DEBUG) System.out.println(WhoisHistoryService.class.getSimpleName() + ": No registrable domains");
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
				lastRunSuccessful = true;
			} catch(ThreadDeath TD) {
				throw TD;
			} catch(Throwable T) {
				logger.log(Level.SEVERE, null, T);
			}
		}
	};

	private static final String COMMAND = "/usr/bin/whois";

	/**
	 * Performs a whois lookup for a zone.  This is not cross-platform capable at this time.
	 */
	private static String getWhoisOutput(DomainName registrableDomain) throws IOException {
		String lower = registrableDomain.toLowerCase();
		StringBuilder sb = new StringBuilder();
		sb.append("---------- COMMAND ---------\n");
		sb.append(COMMAND).append(" -H ").append(lower).append('\n');
		ProcessResult result = ProcessResult.exec(COMMAND, "-H", lower);
		int retVal = result.getExitVal();
		if(sb.charAt(sb.length()-1) != '\n') sb.append('\n');
		// TODO: Exit status should probably be stored in the WhoisHistory table itself
		// Or separate columns for each: command, output, error, exit status
		sb.append("------- EXIT STATUS --------\n");
		sb.append(retVal).append('\n');
		String stdout = result.getStdout();
		if(!stdout.isEmpty()) {
			if(sb.charAt(sb.length()-1) != '\n') sb.append('\n');
			sb.append("---------- OUTPUT ----------\n");
			sb.append(stdout);
		}
		String stderr = result.getStderr();
		if(!stderr.isEmpty()) {
			if(sb.charAt(sb.length()-1) != '\n') sb.append('\n');
			sb.append("---------- ERROR -----------\n");
			sb.append(stderr);
		}
		return sb.toString();
	}

	/**
	 * Gets the set of all unique registrable domains (single domain label + public suffix) and accounts.
	 * Merges the results of calling {@link WhoisHistoryDomainLocator#getWhoisHistoryDomains(com.aoindustries.dbc.DatabaseConnection)}
	 * on all {@link MasterService services}.
	 */
	private Map<DomainName,Set<AccountingCode>> getWhoisHistoryDomains(DatabaseConnection conn) throws IOException, SQLException {
		Map<DomainName,Set<AccountingCode>> merged = new HashMap<>();
		for(WhoisHistoryDomainLocator locator : MasterServer.getServices(WhoisHistoryDomainLocator.class)) {
			for(Map.Entry<DomainName,Set<AccountingCode>> entry : locator.getWhoisHistoryDomains(conn).entrySet()) {
				DomainName registrableDomain = entry.getKey();
				Set<AccountingCode> accounts = merged.get(registrableDomain);
				if(accounts == null) merged.put(registrableDomain, accounts = new LinkedHashSet<>());
				accounts.addAll(entry.getValue());
				
			}
		}
		return merged;
	}
	// </editor-fold>

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
	// TODO: Should this be a getObject handler?  Or a new getColumnHandler for when certain columns are not fetched in main query?
	public String getWhoisHistoryOutput(DatabaseConnection conn, RequestSource source, int id) throws IOException, SQLException {
		AccountingCode accounting = getBusinessForWhoisHistory(conn, id);
		BusinessHandler.checkAccessBusiness(conn, source, "getWhoisHistoryOutput", accounting);
		return conn.executeStringQuery("select whois_output from billing.\"WhoisHistory\" where id=?", id);
	}

	// <editor-fold desc="GetTableHandler" defaultstate="collapsed">
	@Override
	public TableHandler.GetTableHandler startGetTableHandler() {
		return new TableHandler.GetTableHandlerByRole() {

			@Override
			public Set<Table.TableID> getTableIds() {
				return EnumSet.of(Table.TableID.WHOIS_HISTORY);
			}

			@Override
			protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
				MasterServer.writeObjects(
					conn,
					source,
					out,
					provideProgress,
					CursorMode.FETCH,
					new WhoisHistory(),
					"select id, time, accounting, zone from billing.\"WhoisHistory\""
				);
			}

			@Override
			protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
				// The servers don't need access to this information
				MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
			}

			@Override
			protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
				MasterServer.writeObjects(
					conn,
					source,
					out,
					provideProgress,
					CursorMode.FETCH,
					new WhoisHistory(),
					"select\n"
					+ "  wh.id,\n"
					+ "  wh.time,\n"
					+ "  wh.accounting,\n"
					+ "  wh.zone\n"
					+ "from\n"
					+ "  account.\"Username\" un,\n"
					+ "  billing.\"Package\" pk,\n"
					+ TableHandler.BU1_PARENTS_JOIN
					+ "  billing.\"WhoisHistory\" wh\n"
					+ "where\n"
					+ "  un.username=?\n"
					+ "  and un.package=pk.name\n"
					+ "  and (\n"
					+ TableHandler.PK_BU1_PARENTS_WHERE
					+ "  )\n"
					+ "  and bu1.accounting=wh.accounting",
					source.getUsername()
				);
			}
		};
	}
	// </editor-fold>
}
