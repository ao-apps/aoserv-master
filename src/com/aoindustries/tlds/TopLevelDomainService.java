/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.tlds;

import com.aoindustries.aoserv.master.LogFactory;
import com.aoindustries.aoserv.master.MasterDatabase;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.MasterService;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.CronJobScheduleMode;
import com.aoindustries.cron.Schedule;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.util.StringUtility;
import com.aoindustries.util.logging.ProcessTimer;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Synchronizes the database table from the auto-updating Java API.
 * <p>
 * This is based on TopLevelDomain-import.sql from the ao-tlds project.
 * </p>
 *
 * @author  AO Industries, Inc.
 */
public class TopLevelDomainService implements MasterService {

	private static final Logger logger = LogFactory.getLogger(TopLevelDomainService.class);

	@Override
	public void start() {
		CronDaemon.addCronJob(cronJob, logger);
		// Run at start-up, too
		CronDaemon.runImmediately(cronJob);
	}

	// <editor-fold desc="CronJob" defaultstate="collapsed">
	private final CronJob cronJob = new CronJob() {

		/**
		 * Runs hourly, at 18 minutes after the hour.
		 */
		@Override
		public Schedule getCronJobSchedule() {
			return (minute, hour, dayOfMonth, month, dayOfWeek, year) -> minute == 18;
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
			return Thread.NORM_PRIORITY - 1;
		}

		private volatile long lastUpdatedTime = Long.MIN_VALUE;

		@Override
		public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
			try {
				ProcessTimer timer = new ProcessTimer(
					logger,
					getClass().getName(),
					"runCronJob",
					TopLevelDomainService.class.getSimpleName() + " - Top Level Domain",
					"Synchronizing database tables from auto-updating Java API",
					5L * 60 * 1000, // 5 minutes
					24L * 60 * 60 * 1000 // 24 hours
				);
				try {
					MasterServer.executorService.submit(timer);

					// Get the current TopLevelDomains snapshot
					TopLevelDomain.Snapshot snapshot = TopLevelDomain.getSnapshot();
					long lastUpdatedMillis = snapshot.getLastUpdatedTime();
					if(lastUpdatedTime == Long.MIN_VALUE || lastUpdatedMillis != lastUpdatedTime) {
						// Start the transaction
						DatabaseConnection conn = MasterDatabase.getDatabase().createDatabaseConnection();
						try {
							boolean connRolledBack = false;
							try {
								Timestamp lastUpdatedTimestamp = new Timestamp(lastUpdatedMillis);
								// Check if this update is already in the database
								if(
									conn.executeBooleanQuery(
										"SELECT EXISTS (\n"
										+ "  SELECT * FROM \"com.aoindustries.tlds\".\"TopLevelDomain.Log\" WHERE \"lastUpdatedTime\"=?\n"
										+ ")",
										lastUpdatedTimestamp
									)
								) {
									if(logger.isLoggable(Level.FINE)) {
										logger.log(
											Level.FINE,
											"The database already contains the update dated \""
											+ DateFormat.getDateTimeInstance().format(lastUpdatedTimestamp)
											+ "\""
										);
									}
								} else {
									List<String> topLevelDomains = snapshot.getTopLevelDomains();
									if(topLevelDomains.isEmpty()) throw new AssertionError("topLevelDomains is empty");
									// Create and populate a temporary table
									conn.executeUpdate(
										"CREATE TEMPORARY TABLE \"TopLevelDomain_import\" (\n"
										+ "  label CITEXT PRIMARY KEY\n"
										+ ")");
									StringBuilder insert = new StringBuilder();
									insert.append("INSERT INTO \"TopLevelDomain_import\" VALUES ");
									for(int i = 0, size = topLevelDomains.size(); i < size; i++) {
										if(i != 0) insert.append(',');
										insert.append("(?)");
									}
									conn.executeUpdate(insert.toString(), topLevelDomains.toArray());
									// Delete old entries
									int deleted = conn.executeUpdate(
										"DELETE FROM \"com.aoindustries.tlds\".\"TopLevelDomain\" WHERE label NOT IN (\n"
										+ "  SELECT label FROM \"TopLevelDomain_import\"\n"
										+ ")");
									// Delete old entries where case changed
									int delete_for_update = conn.executeUpdate(
										"DELETE FROM \"com.aoindustries.tlds\".\"TopLevelDomain\" WHERE label::text NOT IN (\n"
										+ "  SELECT label::text FROM \"TopLevelDomain_import\"\n"
										+ ")");
									// Add new entries
									int inserted = conn.executeUpdate(
										"INSERT INTO \"com.aoindustries.tlds\".\"TopLevelDomain\"\n"
										+ "SELECT * FROM \"TopLevelDomain_import\" WHERE label NOT IN (\n"
										+ "  SELECT label FROM \"com.aoindustries.tlds\".\"TopLevelDomain\"\n"
										+ ")");
									// Drop temp table
									conn.executeUpdate("DROP TABLE \"TopLevelDomain_import\"");
									// Add Log entry
									conn.executeUpdate(
										"INSERT INTO \"com.aoindustries.tlds\".\"TopLevelDomain.Log\" VALUES (\n"
										+ "  ?,\n"
										+ "  ?,\n"
										+ "  ?,\n"
										+ "  ?,\n"
										+ "  ?,\n"
										+ "  ?,\n"
										+ "  ?\n"
										+ ")",
										lastUpdatedTimestamp,
										snapshot.getLastUpdateSuccessful(),
										new Timestamp(snapshot.getLastSuccessfulUpdateTime()),
										StringUtility.join(snapshot.getComments(), "\n"),
										inserted - delete_for_update,
										delete_for_update,
										deleted
									);
									Level level = inserted != 0 || delete_for_update != 0 || deleted != 0 ? Level.INFO : Level.FINE;
									if(logger.isLoggable(level)) {
										logger.log(
											level,
											"Database modified from self-updating Java API update dated \""
											+ DateFormat.getDateTimeInstance().format(lastUpdatedTimestamp)
											+ "\": inserted=" + (inserted - delete_for_update)
											+ ", updated=" + delete_for_update
											+ ", deleted=" + deleted
										);
									}
								}
								lastUpdatedTime = lastUpdatedMillis;
							} catch(RuntimeException err) {
								if(conn.rollback()) {
									connRolledBack = true;
								}
								throw err;
							} catch(SQLException err) {
								if(conn.rollbackAndClose()) {
									connRolledBack = true;
								}
								throw err;
							} finally {
								if(!connRolledBack && !conn.isClosed()) conn.commit();
							}
						} finally {
							conn.releaseConnection();
						}
					}
				} finally {
					timer.finished();
				}
			} catch(ThreadDeath TD) {
				throw TD;
			} catch(Throwable T) {
				logger.log(Level.SEVERE, null, T);
			}
		}
	};
	// </editor-fold>
}
