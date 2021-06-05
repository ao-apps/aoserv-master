/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2003-2013, 2015, 2017, 2018, 2019, 2020, 2021  AO Industries, Inc.
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
 * along with aoserv-master.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.master;

import com.aoapps.cron.CronDaemon;
import com.aoapps.cron.CronJob;
import com.aoapps.cron.Schedule;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.logging.ProcessTimer;
import com.aoapps.lang.util.ErrorPrinter;
import com.aoindustries.aoserv.client.backup.BackupReport;
import com.aoindustries.aoserv.client.schema.Table;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Automatically generates various reports on a regular basis.  These reports are then
 * used by the accounting system to charge the appropriate amount.  These reports may be missed
 * and will not be created when missed.  Anything depending on these reports should get
 * its information from the reports that are available without depending on all reports
 * being present.  It is an acceptable error condition if not a single report in a month
 * has occurred.
 *
 * @author  AO Industries, Inc.
 */
final public class ReportGenerator implements CronJob {

	private static final Logger logger = Logger.getLogger(ReportGenerator.class.getName());

	/**
	 * The maximum time for a backup reporting.
	 */
	private static final long BACKUP_REPORT_MAX_TIME=2L*60*60*1000;

	/**
	 * The interval in which the administrators will be reminded.
	 */
	private static final long TIMER_REMINDER_INTERVAL=12L*60*60*1000;

	static class TempBackupReport {
		int host;
		int packageNum;
		int fileCount;
		long diskSize;
	}

	private static boolean started=false;

	@SuppressWarnings("UseOfSystemOutOrSystemErr")
	public static void start() {
		synchronized(System.out) {
			if(!started) {
				System.out.print("Starting " + ReportGenerator.class.getSimpleName() + ": ");
				CronDaemon.addCronJob(new ReportGenerator(), logger);
				started=true;
				System.out.println("Done");
			}
		}
	}

	private ReportGenerator() {
	}

	/**
	 * Runs at {@link BackupReport#BACKUP_REPORT_HOUR}:{@link BackupReport#BACKUP_REPORT_MINUTE} am daily.
	 */
	private static final Schedule schedule = (minute, hour, dayOfMonth, month, dayOfWeek, year) ->
		minute==BackupReport.BACKUP_REPORT_MINUTE
		&& hour==BackupReport.BACKUP_REPORT_HOUR;

	@Override
	public Schedule getSchedule() {
		return schedule;
	}

	@Override
	public int getThreadPriority() {
		return Thread.NORM_PRIORITY-2;
	}

	@Override
	public void run(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
		try {
			try (
				ProcessTimer timer=new ProcessTimer(
					logger,
					ReportGenerator.class.getName(),
					"runCronJob",
					"Backup Report Generator",
					"Generating contents for backup.BackupReport",
					BACKUP_REPORT_MAX_TIME,
					TIMER_REMINDER_INTERVAL
				)
			) {
				MasterServer.executorService.submit(timer);

				// Start the transaction
				try (DatabaseConnection conn = MasterDatabase.getDatabase().connect()) {
					InvalidateList invalidateList = new InvalidateList();
					// Do not make the run twice in one day
					if(
						conn.queryBoolean(
							"select\n"
							+ "  not exists (\n"
							+ "    select\n"
							+ "      *\n"
							+ "    from\n"
							+ "      backup.\"BackupReport\"\n"
							+ "    where\n"
							+ "      CURRENT_DATE = date\n"
							+ "  )"
						)
					) {
						// HashMap keyed on host, containing HashMaps keyed on package, containing TempBackupReport objects
						Map<Integer, Map<Integer, TempBackupReport>> stats=new HashMap<>();

						/* TODO: Implement as calls to the aoserv daemons to get the quota reports
						String currentSQL = null;
						try (Statement stmt = conn.getConnection(true).createStatement()) {
							// First, count up the total number of files per host and per package
							conn.incrementQueryCount();
							try (ResultSet results = stmt.executeQuery(currentSQL = "select server, package, count(*) from file_backups group by server, package")) {
								while(results.next()) {
									int host=results.getInt(1);
									int packageNum=results.getInt(2);
									int fileCount=results.getInt(3);

									TempBackupReport tbr=new TempBackupReport();
									tbr.host=host;
									tbr.packageNum=packageNum;
									tbr.fileCount=fileCount;

									Integer hostInteger=Integer.valueOf(host);
									Map<Integer, TempBackupReport> packages=stats.get(hostInteger);
									if(packages==null) stats.put(hostInteger, packages=new HashMap<Integer, TempBackupReport>());
									packages.put(Integer.valueOf(packageNum), tbr);
								}
							}

							// Count up the data sizes by host and package
							conn.incrementQueryCount();
							try (
								ResultSet results = stmt.executeQuery(
									currentSQL =
									  "select\n"
									+ "  fb.server,\n"
									+ "  fb.package,\n"
									+ "  sum(bd.data_size),\n"
									+ "  sum(coalesce(bd.compressed_size, bd.data_size)),\n"
									+ "  sum(\n"
									+ "    case when\n"
									+ "      (coalesce(bd.compressed_size, bd.data_size)%(4096::int8))=0\n"
									+ "    then\n"
									+ "      coalesce(bd.compressed_size, bd.data_size)\n"
									+ "    else\n"
									+ "      ((coalesce(bd.compressed_size, bd.data_size)/4096)+1)*4096\n"
									+ "    end\n"
									+ "  )\n"
									+ "from\n"
									+ "  (\n"
									+ "    select\n"
									+ "      server,\n"
									+ "      package,\n"
									+ "      backup_data\n"
									+ "    from\n"
									+ "      file_backups\n"
									+ "    where\n"
									+ "      backup_data is not null\n"
									+ "    union select\n"
									+ "      ao_server,\n"
									+ "      package,\n"
									+ "      backup_data\n"
									+ "    from\n"
									+ "      interbase_backups\n"
									+ "    union select\n"
									+ "      ms.ao_server,\n"
									+ "      mb.package,\n"
									+ "      mb.backup_data\n"
									+ "    from\n"
									+ "      mysql_backups mb,\n"
									+ "      mysql.\"Server\" ms\n"
									+ "    where\n"
									+ "      mb.mysql_server=ms.bind\n"
									+ "    union select\n"
									+ "      ps.ao_server,\n"
									+ "      pb.package,\n"
									+ "      pb.backup_data\n"
									+ "    from\n"
									+ "      postgres_backups pb\n"
									+ "      INNER JOIN postgresql.\"Server\" ps ON pb.postgres_server = ps.bind\n"
									+ "  ) as fb,\n"
									+ "  backup_data bd\n"
									+ "where\n"
									+ "  fb.backup_data=bd.id\n"
									+ "  and bd.is_stored\n"
									+ "group by\n"
									+ "  fb.server,\n"
									+ "  fb.package"
								)
							) {
								while(results.next()) {
									int host=results.getInt(1);
									int packageNum=results.getInt(2);
									long uncompressedSize=results.getLong(3);
									long compressedSize=results.getLong(4);
									long diskSize=results.getLong(5);

									Integer hostInteger=Integer.valueOf(host);
									Map<Integer, TempBackupReport> packages=stats.get(hostInteger);
									if(packages==null) stats.put(hostInteger, packages=new HashMap<Integer, TempBackupReport>());
									Integer packageInteger=Integer.valueOf(packageNum);
									TempBackupReport tbr=(TempBackupReport)packages.get(packageInteger);
									if(tbr==null) {
										tbr=new TempBackupReport();
										tbr.host=host;
										tbr.packageNum=packageNum;
										packages.put(packageInteger, tbr);
									}
									tbr.uncompressedSize=uncompressedSize;
									tbr.compressedSize=compressedSize;
									tbr.diskSize=diskSize;
								}
							}
						} catch(Error | RuntimeException | SQLException e) {
							ErrorPrinter.addSQL(e, currentSQL);
							throw e;
						}*/

						// Add these stats to the table
						try (PreparedStatement pstmt = conn.getConnection().prepareStatement("INSERT INTO backup.\"BackupReport\" VALUES (default,?,?,CURRENT_DATE,?,?::int8);")) {
							try {
								Iterator<Integer> hostKeys = stats.keySet().iterator();
								while(hostKeys.hasNext()) {
									Map<Integer, TempBackupReport> packages = stats.get(hostKeys.next());
									Iterator<Integer> packageKeys=packages.keySet().iterator();
									while(packageKeys.hasNext()) {
										TempBackupReport tbr=packages.get(packageKeys.next());
										pstmt.setInt(1, tbr.host);
										pstmt.setInt(2, tbr.packageNum);
										pstmt.setInt(3, tbr.fileCount);
										pstmt.setLong(4, tbr.diskSize);
										pstmt.addBatch();
									}
								}
								pstmt.executeBatch();
							} catch(Error | RuntimeException | SQLException e) {
								ErrorPrinter.addSQL(e, pstmt);
								throw e;
							}
						}

						// Invalidate the table
						invalidateList.addTable(conn, Table.TableID.BACKUP_REPORTS, InvalidateList.allAccounts, InvalidateList.allHosts, false);
					}
					conn.commit();
					MasterServer.invalidateTables(conn, invalidateList, null);
				}
			}
		} catch(ThreadDeath TD) {
			throw TD;
		} catch(Throwable T) {
			logger.log(Level.SEVERE, null, T);
		}
	}
}
