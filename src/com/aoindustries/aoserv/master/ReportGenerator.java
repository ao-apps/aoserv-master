/*
 * Copyright 2003-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.BackupReport;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.CronJobScheduleMode;
import com.aoindustries.cron.Schedule;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.util.logging.ProcessTimer;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
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

	private static final Logger logger = LogFactory.getLogger(ReportGenerator.class);

	/**
	 * The maximum time for a backup reporting.
	 */
	private static final long BACKUP_REPORT_MAX_TIME=2L*60*60*1000;

	/**
	 * The interval in which the administrators will be reminded.
	 */
	private static final long TIMER_REMINDER_INTERVAL=12L*60*60*1000;

	static class TempBackupReport {
		int server;
		int packageNum;
		int fileCount;
		long diskSize;
	}

	private static boolean started=false;

	public static void start() {
		synchronized(System.out) {
			if(!started) {
				System.out.print("Starting ReportGenerator: ");
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
	public Schedule getCronJobSchedule() {
		return schedule;
	}

	@Override
	public CronJobScheduleMode getCronJobScheduleMode() {
		return CronJobScheduleMode.SKIP;
	}

	@Override
	public String getCronJobName() {
		return "ReportGenerator";
	}

	@Override
	public int getCronJobThreadPriority() {
		return Thread.NORM_PRIORITY-2;
	}

	@Override
	public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
		try {
			ProcessTimer timer=new ProcessTimer(
				logger,
				MasterServer.getRandom(),
				ReportGenerator.class.getName(),
				"runCronJob",
				"Backup Report Generator",
				"Generating contents for backup.BackupReport",
				BACKUP_REPORT_MAX_TIME,
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
						long currentTimeMillis=System.currentTimeMillis();
						Timestamp now=new Timestamp(currentTimeMillis);

						// Do not make the run twice in one day
						if(
							conn.executeBooleanQuery(
								"select\n"
								+ "  (\n"
								+ "    select\n"
								+ "      pkey\n"
								+ "    from\n"
								+ "      backup.\"BackupReport\"\n"
								+ "    where\n"
								+ "      ?::date=date\n"
								+ "    limit 1\n"
								+ "  ) is null",
								now
							)
						) {
							// HashMap keyed on server, containing HashMaps keyed on package, containing TempBackupReport objects
							Map<Integer,Map<Integer,TempBackupReport>> stats=new HashMap<>();

							/* TODO: Implement as calls to the aoserv daemons to get the quota reports
							String sqlString=null;
							Statement stmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
							try {
								// First, count up the total number of files per server and per package
								conn.incrementQueryCount();
								ResultSet results=stmt.executeQuery(sqlString="select server, package, count(*) from file_backups group by server, package");
								try {
									while(results.next()) {
										int server=results.getInt(1);
										int packageNum=results.getInt(2);
										int fileCount=results.getInt(3);

										TempBackupReport tbr=new TempBackupReport();
										tbr.server=server;
										tbr.packageNum=packageNum;
										tbr.fileCount=fileCount;

										Integer serverInteger=Integer.valueOf(server);
										Map<Integer,TempBackupReport> packages=stats.get(serverInteger);
										if(packages==null) stats.put(serverInteger, packages=new HashMap<Integer,TempBackupReport>());
										packages.put(Integer.valueOf(packageNum), tbr);
									}
								} finally {
									results.close();
								}

								// Count up the data sizes by server and package
								conn.incrementQueryCount();
								results=stmt.executeQuery(
									sqlString=
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
									+ "      mysql.\"MysqlServer\" ms\n"
									+ "    where\n"
									+ "      mb.mysql_server=ms.net_bind\n"
									+ "    union select\n"
									+ "      ps.ao_server,\n"
									+ "      pb.package,\n"
									+ "      pb.backup_data\n"
									+ "    from\n"
									+ "      postgres_backups pb,\n"
									+ "      postgres_servers ps\n"
									+ "    where\n"
									+ "      pb.postgres_server=ps.pkey\n"
									+ "  ) as fb,\n"
									+ "  backup_data bd\n"
									+ "where\n"
									+ "  fb.backup_data=bd.pkey\n"
									+ "  and bd.is_stored\n"
									+ "group by\n"
									+ "  fb.server,\n"
									+ "  fb.package"
								);
								try {
									while(results.next()) {
										int server=results.getInt(1);
										int packageNum=results.getInt(2);
										long uncompressedSize=results.getLong(3);
										long compressedSize=results.getLong(4);
										long diskSize=results.getLong(5);

										Integer serverInteger=Integer.valueOf(server);
										Map<Integer,TempBackupReport> packages=stats.get(serverInteger);
										if(packages==null) stats.put(serverInteger, packages=new HashMap<Integer,TempBackupReport>());
										Integer packageInteger=Integer.valueOf(packageNum);
										TempBackupReport tbr=(TempBackupReport)packages.get(packageInteger);
										if(tbr==null) {
											tbr=new TempBackupReport();
											tbr.server=server;
											tbr.packageNum=packageNum;
											packages.put(packageInteger, tbr);
										}
										tbr.uncompressedSize=uncompressedSize;
										tbr.compressedSize=compressedSize;
										tbr.diskSize=diskSize;
									}
								} finally {
									results.close();
								}
							} catch(SQLException err) {
								System.err.println("Error from query: "+sqlString);
								throw err;
							} finally {
								stmt.close();
							}*/

							// Add these stats to the table
							PreparedStatement pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("INSERT INTO backup.\"BackupReport\" VALUES (default,?,?,?::date,?,?::int8);");
							try {
								Iterator<Integer> serverKeys=stats.keySet().iterator();
								while(serverKeys.hasNext()) {
									Map<Integer,TempBackupReport> packages=stats.get(serverKeys.next());
									Iterator<Integer> packageKeys=packages.keySet().iterator();
									while(packageKeys.hasNext()) {
										TempBackupReport tbr=packages.get(packageKeys.next());
										pstmt.setInt(1, tbr.server);
										pstmt.setInt(2, tbr.packageNum);
										pstmt.setTimestamp(3, now);
										pstmt.setInt(4, tbr.fileCount);
										pstmt.setLong(5, tbr.diskSize);
										pstmt.addBatch();
									}
								}
								pstmt.executeBatch();
							} catch(SQLException err) {
								System.err.println("Error from update: "+pstmt.toString());
								throw err;
							} finally {
								pstmt.close();
							}

							// Invalidate the table
							invalidateList.addTable(conn, SchemaTable.TableID.BACKUP_REPORTS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
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
}
