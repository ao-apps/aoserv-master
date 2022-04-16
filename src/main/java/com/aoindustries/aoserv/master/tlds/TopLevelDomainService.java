/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019, 2020, 2021, 2022  AO Industries, Inc.
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

package com.aoindustries.aoserv.master.tlds;

import com.aoapps.cron.CronDaemon;
import com.aoapps.cron.CronJob;
import com.aoapps.cron.Schedule;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.logging.ProcessTimer;
import com.aoapps.lang.Strings;
import com.aoapps.tlds.TopLevelDomain;
import com.aoindustries.aoserv.master.MasterDatabase;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.MasterService;
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

	private static final Logger logger = Logger.getLogger(TopLevelDomainService.class.getName());

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
		public Schedule getSchedule() {
			return (minute, hour, dayOfMonth, month, dayOfWeek, year) -> minute == 18;
		}

		@Override
		public int getThreadPriority() {
			return Thread.NORM_PRIORITY - 1;
		}

		private volatile long lastUpdatedTime = Long.MIN_VALUE;

		@Override
		@SuppressWarnings({"UseSpecificCatch", "TooBroadCatch"})
		public void run(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
			try {
				try (
					ProcessTimer timer = new ProcessTimer(
						logger,
						getClass().getName(),
						"runCronJob",
						TopLevelDomainService.class.getSimpleName() + " - Top Level Domain",
						"Synchronizing database tables from auto-updating Java API",
						5L * 60 * 1000, // 5 minutes
						24L * 60 * 60 * 1000 // 24 hours
					)
				) {
					MasterServer.executorService.submit(timer);

					// Get the current TopLevelDomains snapshot
					TopLevelDomain.Snapshot snapshot = TopLevelDomain.getSnapshot();
					long lastUpdatedMillis = snapshot.getLastUpdatedTime();
					if(lastUpdatedTime == Long.MIN_VALUE || lastUpdatedMillis != lastUpdatedTime) {
						// Start the transaction
						try (DatabaseConnection conn = MasterDatabase.getDatabase().connect()) {
							Timestamp lastUpdatedTimestamp = new Timestamp(lastUpdatedMillis);
							// Check if this update is already in the database
							if(
								conn.queryBoolean(
									"SELECT EXISTS (\n"
									+ "  SELECT * FROM \"com.aoapps.tlds\".\"TopLevelDomain.Log\" WHERE \"lastUpdatedTime\"=?\n"
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
								conn.update(
									"CREATE TEMPORARY TABLE \"TopLevelDomain_import\" (\n"
									+ "  label CITEXT PRIMARY KEY\n"
									+ ")");
								StringBuilder insert = new StringBuilder();
								insert.append("INSERT INTO \"TopLevelDomain_import\" VALUES ");
								for(int i = 0, size = topLevelDomains.size(); i < size; i++) {
									if(i != 0) insert.append(',');
									insert.append("(?)");
								}
								conn.update(insert.toString(), topLevelDomains.toArray());
								// Delete old entries
								int deleted = conn.update(
									"DELETE FROM \"com.aoapps.tlds\".\"TopLevelDomain\" WHERE label NOT IN (\n"
									+ "  SELECT label FROM \"TopLevelDomain_import\"\n"
									+ ")");
								// Delete old entries where case changed
								int delete_for_update = conn.update(
									"DELETE FROM \"com.aoapps.tlds\".\"TopLevelDomain\" WHERE label::text NOT IN (\n"
									+ "  SELECT label::text FROM \"TopLevelDomain_import\"\n"
									+ ")");
								// Add new entries
								int inserted = conn.update(
									"INSERT INTO \"com.aoapps.tlds\".\"TopLevelDomain\"\n"
									+ "SELECT * FROM \"TopLevelDomain_import\" WHERE label NOT IN (\n"
									+ "  SELECT label FROM \"com.aoapps.tlds\".\"TopLevelDomain\"\n"
									+ ")");
								// Drop temp table
								conn.update("DROP TABLE \"TopLevelDomain_import\"");
								// Add Log entry
								conn.update(
									"INSERT INTO \"com.aoapps.tlds\".\"TopLevelDomain.Log\" VALUES (\n"
									+ "  ?,\n"
									+ "  ?,\n"
									+ "  ?,\n"
									+ "  ?,\n"
									+ "  ?,\n"
									+ "  ?,\n"
									+ "  ?,\n"
									+ "  ?\n"
									+ ")",
									lastUpdatedTimestamp,
									snapshot.isBootstrap(),
									snapshot.getLastUpdateSuccessful(),
									new Timestamp(snapshot.getLastSuccessfulUpdateTime()),
									Strings.join(snapshot.getComments(), "\n"),
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
							conn.commit();
						}
					}
				}
			} catch(ThreadDeath td) {
				throw td;
			} catch(Throwable t) {
				logger.log(Level.SEVERE, null, t);
			}
		}
	};
	// </editor-fold>
}
