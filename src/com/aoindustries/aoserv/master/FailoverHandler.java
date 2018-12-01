/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.backup.FileReplicationLog;
import com.aoindustries.aoserv.client.linux.Server;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.daemon.client.AOServDaemonProtocol;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.CronJobScheduleMode;
import com.aoindustries.cron.Schedule;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.BitRateProvider;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.net.HostAddress;
import com.aoindustries.util.IntList;
import com.aoindustries.util.Tuple2;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The <code>FailoverHandler</code> handles all the accesses to the failover tables.
 *
 * @author  AO Industries, Inc.
 */
final public class FailoverHandler implements CronJob {

	private static final Logger logger = LogFactory.getLogger(FailoverHandler.class);

	public static int addFailoverFileLog(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int replication,
		long startTime,
		long endTime,
		int scanned,
		int updated,
		long bytes,
		boolean isSuccessful
	) throws IOException, SQLException {
		//String mustring = source.getUsername();
		//User mu = MasterServer.getUser(conn, mustring);
		//if (mu==null) throw new SQLException("User "+mustring+" is not master user and may not access backup.FileReplicationLog.");

		// The server must be an exact package match to allow adding log entries
		int server=getFromServerForFailoverFileReplication(conn, replication);
		AccountingCode userPackage = UsernameHandler.getPackageForUsername(conn, source.getUsername());
		AccountingCode serverPackage = PackageHandler.getNameForPackage(conn, ServerHandler.getPackageForServer(conn, server));
		if(!userPackage.equals(serverPackage)) throw new SQLException("userPackage!=serverPackage: may only set backup.FileReplicationLog for servers that have the same package as the business_administrator adding the log entry");
		//ServerHandler.checkAccessServer(conn, source, "add_failover_file_log", server);

		int id = conn.executeIntUpdate(
			"INSERT INTO\n"
			+ "  backup.\"FileReplicationLog\"\n"
			+ "VALUES (\n"
			+ "  default,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?\n"
			+ ") RETURNING id",
			replication,
			new Timestamp(startTime),
			new Timestamp(endTime),
			scanned,
			updated,
			bytes,
			isSuccessful
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.FAILOVER_FILE_LOG,
			ServerHandler.getBusinessesForServer(conn, server),
			server,
			false
		);
		return id;
	}

	public static void setFailoverFileReplicationBitRate(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		Long bitRate
	) throws IOException, SQLException {
		if(
			bitRate!=null
			&& bitRate<BitRateProvider.MINIMUM_BIT_RATE
		) throw new SQLException("Bit rate too low: "+bitRate+"<"+BitRateProvider.MINIMUM_BIT_RATE);

		// The server must be an exact package match to allow setting the bit rate
		int server=getFromServerForFailoverFileReplication(conn, id);
		AccountingCode userPackage = UsernameHandler.getPackageForUsername(conn, source.getUsername());
		AccountingCode serverPackage = PackageHandler.getNameForPackage(conn, ServerHandler.getPackageForServer(conn, server));
		if(!userPackage.equals(serverPackage)) throw new SQLException("userPackage!=serverPackage: may only set backup.FileReplication.max_bit_rate for servers that have the same package as the business_administrator setting the bit rate");

		if(bitRate==null) conn.executeUpdate("update backup.\"FileReplication\" set max_bit_rate=null where id=?", id);
		else conn.executeUpdate("update backup.\"FileReplication\" set max_bit_rate=? where id=?", bitRate, id);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.FAILOVER_FILE_REPLICATIONS,
			ServerHandler.getBusinessesForServer(conn, server),
			server,
			false
		);
	}

	public static void setFailoverFileSchedules(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int replication,
		List<Short> hours,
		List<Short> minutes
	) throws IOException, SQLException {
		// The server must be an exact package match to allow setting the schedule
		int server=getFromServerForFailoverFileReplication(conn, replication);
		AccountingCode userPackage = UsernameHandler.getPackageForUsername(conn, source.getUsername());
		AccountingCode serverPackage = PackageHandler.getNameForPackage(conn, ServerHandler.getPackageForServer(conn, server));
		if(!userPackage.equals(serverPackage)) throw new SQLException("userPackage!=serverPackage: may only set backup.FileReplicationSchedule for servers that have the same package as the business_administrator setting the schedule");

		// If not modified, invalidation will not be performed
		boolean modified = false;

		// Get the list of all the ids that currently exist
		IntList ids = conn.executeIntListQuery("select id from backup.\"FileReplicationSchedule\" where replication=?", replication);
		int size = hours.size();
		for(int c=0;c<size;c++) {
			// If it exists, remove id from the list, otherwise add
			short hour = hours.get(c);
			short minute = minutes.get(c);
			int existingPkey = conn.executeIntQuery(
				"select coalesce((select id from backup.\"FileReplicationSchedule\" where replication=? and hour=? and minute=?), -1)",
				replication,
				hour,
				minute
			);
			if(existingPkey==-1) {
				// Doesn't exist, add
				conn.executeUpdate("insert into backup.\"FileReplicationSchedule\" (replication, hour, minute, enabled) values(?,?,?,true)", replication, hour, minute);
				modified = true;
			} else {
				// Remove from the list that will be removed
				if(!ids.removeByValue(existingPkey)) throw new SQLException("ids doesn't contain id="+existingPkey);
			}
		}
		// Delete the unmatched ids
		if(ids.size()>0) {
			for(int c=0,len=ids.size(); c<len; c++) {
				conn.executeUpdate("delete from backup.\"FileReplicationSchedule\" where id=?", ids.getInt(c));
			}
			modified = true;
		}

		// Notify all clients of the update
		if(modified) {
			invalidateList.addTable(
				conn,
				Table.TableID.FAILOVER_FILE_SCHEDULE,
				ServerHandler.getBusinessesForServer(conn, server),
				server,
				false
			);
		}
	}

	public static void setFileBackupSettingsAllAtOnce(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int replication,
		List<String> paths,
		List<Boolean> backupEnableds,
		List<Boolean> requireds
	) throws IOException, SQLException {
		// The server must be an exact package match to allow setting the schedule
		int server=getFromServerForFailoverFileReplication(conn, replication);
		AccountingCode userPackage = UsernameHandler.getPackageForUsername(conn, source.getUsername());
		AccountingCode serverPackage = PackageHandler.getNameForPackage(conn, ServerHandler.getPackageForServer(conn, server));
		if(!userPackage.equals(serverPackage)) throw new SQLException("userPackage!=serverPackage: may only set backup.FileReplicationSetting for servers that have the same package as the business_administrator making the settings");

		// If not modified, invalidation will not be performed
		boolean modified = false;

		// Get the list of all the ids that currently exist
		IntList ids = conn.executeIntListQuery("select id from backup.\"FileReplicationSetting\" where replication=?", replication);
		int size = paths.size();
		for(int c=0;c<size;c++) {
			// If it exists, remove id from the list, otherwise add
			String path = paths.get(c);
			boolean backupEnabled = backupEnableds.get(c);
			boolean required = requireds.get(c);
			int existingPkey = conn.executeIntQuery(
				"select coalesce((select id from backup.\"FileReplicationSetting\" where replication=? and path=?), -1)",
				replication,
				path
			);
			if(existingPkey==-1) {
				// Doesn't exist, add
				conn.executeUpdate(
					"insert into backup.\"FileReplicationSetting\" (replication, path, backup_enabled, required) values(?,?,?,?)",
					replication,
					path,
					backupEnabled,
					required
				);
				modified = true;
			} else {
				// Update the flags if either doesn't match
				if(
					conn.executeUpdate(
						"update backup.\"FileReplicationSetting\" set backup_enabled=?, required=? where id=? and not (backup_enabled=? and required=?)",
						backupEnabled,
						required,
						existingPkey,
						backupEnabled,
						required
					)==1
				) modified = true;

				// Remove from the list that will be removed
				if(!ids.removeByValue(existingPkey)) throw new SQLException("ids doesn't contain id="+existingPkey);
			}
		}
		// Delete the unmatched ids
		if(ids.size()>0) {
			for(int c=0,len=ids.size(); c<len; c++) {
				conn.executeUpdate("delete from backup.\"FileReplicationSetting\" where id=?", ids.getInt(c));
			}
			modified = true;
		}

		// Notify all clients of the update
		if(modified) {
			invalidateList.addTable(
				conn,
				Table.TableID.FILE_BACKUP_SETTINGS,
				ServerHandler.getBusinessesForServer(conn, server),
				server,
				false
			);
		}
	}

	public static int getFromServerForFailoverFileReplication(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeIntQuery("select server from backup.\"FileReplication\" where id=?", id);
	}

	public static int getBackupPartitionForFailoverFileReplication(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeIntQuery("select backup_partition from backup.\"FileReplication\" where id=?", id);
	}

	public static void getFailoverFileLogs(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataOutputStream out,
		int replication,
		int maxRows
	) throws IOException, SQLException {
		// Check access for the from server
		int fromServer = getFromServerForFailoverFileReplication(conn, replication);
		ServerHandler.checkAccessServer(conn, source, "getFailoverFileLogs", fromServer);

		MasterServer.writeObjects(conn, source, out, false, new FileReplicationLog(), "select * from backup.\"FileReplicationLog\" where replication=? order by start_time desc limit ?", replication, maxRows);
	}

	public static Tuple2<Long,String> getFailoverFileReplicationActivity(
		DatabaseConnection conn,
		RequestSource source,
		int replication
	) throws IOException, SQLException {
		// Check access for the from server
		int fromServer = getFromServerForFailoverFileReplication(conn, replication);
		ServerHandler.checkAccessServer(conn, source, "getFailoverFileReplicationActivity", fromServer);

		// Find where going
		int backupPartition = FailoverHandler.getBackupPartitionForFailoverFileReplication(conn, replication);
		int toServer = BackupHandler.getAOServerForBackupPartition(conn, backupPartition);

		// Contact the server
		return DaemonHandler.getDaemonConnector(conn, toServer).getFailoverFileReplicationActivity(replication);
	}

	/**
	 * Runs at 1:20 am daily.
	 */
	private static final Schedule schedule = (minute, hour, dayOfMonth, month, dayOfWeek, year) -> minute==45 && hour==1;

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
		return "FailoverHandler";
	}

	@Override
	public int getCronJobThreadPriority() {
		return Thread.NORM_PRIORITY-1;
	}

	private static boolean started=false;

	public static void start() {
		synchronized(System.out) {
			if(!started) {
				System.out.print("Starting " + FailoverHandler.class.getSimpleName() + ": ");
				CronDaemon.addCronJob(new FailoverHandler(), logger);
				started=true;
				System.out.println("Done");
			}
		}
	}

	private FailoverHandler() {
	}

	@Override
	public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
		try {
			MasterDatabase.getDatabase().executeUpdate("delete from backup.\"FileReplicationLog\" where end_time <= (now()-'1 year'::interval)");
		} catch(ThreadDeath TD) {
			throw TD;
		} catch(Throwable T) {
			logger.log(Level.SEVERE, null, T);
		}
	}

	public static Server.DaemonAccess requestReplicationDaemonAccess(
		DatabaseConnection conn,
		RequestSource source,
		int id
	) throws IOException, SQLException {
		// Security checks
		//String username=source.getUsername();
		//User masterUser=MasterServer.getUser(conn, username);
		//if(masterUser==null) throw new SQLException("Only master users allowed to request daemon access.");
		// Sometime later we might restrict certain command codes to certain users

		// Current user must have the same exact package as the from server
		AccountingCode userPackage = UsernameHandler.getPackageForUsername(conn, source.getUsername());
		int fromServer=FailoverHandler.getFromServerForFailoverFileReplication(conn, id);
		AccountingCode serverPackage = PackageHandler.getNameForPackage(conn, ServerHandler.getPackageForServer(conn, fromServer));
		if(!userPackage.equals(serverPackage)) throw new SQLException("account.Administrator.username.package!=servers.package.name: Not allowed to request daemon access for FAILOVER_FILE_REPLICATION");
		//ServerHandler.checkAccessServer(conn, source, "requestDaemonAccess", fromServer);

		// The to server must match server
		int backupPartition = FailoverHandler.getBackupPartitionForFailoverFileReplication(conn, id);
		int toServer = BackupHandler.getAOServerForBackupPartition(conn, backupPartition);

		// The overall backup path includes both the toPath and the server name
		String serverName;
		if(ServerHandler.isAOServer(conn, fromServer)) {
			serverName = ServerHandler.getHostnameForAOServer(conn, fromServer).toString();
		} else {
			serverName =
				PackageHandler.getNameForPackage(conn, ServerHandler.getPackageForServer(conn, fromServer))
				+"/"
				+ ServerHandler.getNameForServer(conn, fromServer)
			;
		}

		int quota_gid = conn.executeIntQuery("select coalesce(quota_gid, -1) from backup.\"FileReplication\" where id=?", id);

		// Verify that the backup_partition is the correct type
		boolean isQuotaEnabled = conn.executeBooleanQuery("select bp.quota_enabled from backup.\"FileReplication\" ffr inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where ffr.id=?", id);
		if(quota_gid==-1) {
			if(isQuotaEnabled) throw new SQLException("quota_gid is null when quota_enabled=true: backup.FileReplication.id="+id);
		} else {
			if(!isQuotaEnabled) throw new SQLException("quota_gid is not null when quota_enabled=false: backup.FileReplication.id="+id);
		}

		HostAddress connectAddress = conn.executeObjectQuery(
			ObjectFactories.hostAddressFactory,
			"select connect_address from backup.\"FileReplication\" where id=?",
			id
		);
		return DaemonHandler.grantDaemonAccess(
			conn,
			toServer,
			connectAddress,
			AOServDaemonProtocol.FAILOVER_FILE_REPLICATION,
			Integer.toString(id),
			serverName,
			conn.executeStringQuery("select bp.path from backup.\"FileReplication\" ffr inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where ffr.id=?", id),
			quota_gid==-1 ? null : Integer.toString(quota_gid)
		);
	}
}
