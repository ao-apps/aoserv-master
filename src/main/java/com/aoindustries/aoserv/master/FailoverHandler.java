/*
 * Copyright 2001-2013, 2015, 2017, 2018, 2019, 2020 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.backup.FileReplicationLog;
import com.aoindustries.aoserv.client.linux.Server;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import com.aoindustries.aoserv.daemon.client.AOServDaemonProtocol;
import com.aoindustries.collections.IntList;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.Schedule;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.BitRateProvider;
import com.aoindustries.io.stream.StreamableOutput;
import com.aoindustries.net.HostAddress;
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

	private static final Logger logger = Logger.getLogger(FailoverHandler.class.getName());

	public static int addFileReplicationLog(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int fileReplication,
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
		int host = getFromHostForFileReplication(conn, fileReplication);
		Account.Name userPackage = AccountUserHandler.getPackageForUser(conn, source.getCurrentAdministrator());
		Account.Name serverPackage = PackageHandler.getNameForPackage(conn, NetHostHandler.getPackageForHost(conn, host));
		if(!userPackage.equals(serverPackage)) throw new SQLException("userPackage!=serverPackage: may only set backup.FileReplicationLog for servers that have the same package as the business_administrator adding the log entry");
		//ServerHandler.checkAccessServer(conn, source, "addFileReplicationLog", server);

		int fileReplicationLog = conn.executeIntUpdate(
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
			fileReplication,
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
			NetHostHandler.getAccountsForHost(conn, host),
			host,
			false
		);
		return fileReplicationLog;
	}

	public static void setFileReplicationBitRate(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int fileReplication,
		Long bitRate
	) throws IOException, SQLException {
		if(
			bitRate!=null
			&& bitRate<BitRateProvider.MINIMUM_BIT_RATE
		) throw new SQLException("Bit rate too low: "+bitRate+"<"+BitRateProvider.MINIMUM_BIT_RATE);

		// The server must be an exact package match to allow setting the bit rate
		int host = getFromHostForFileReplication(conn, fileReplication);
		Account.Name userPackage = AccountUserHandler.getPackageForUser(conn, source.getCurrentAdministrator());
		Account.Name serverPackage = PackageHandler.getNameForPackage(conn, NetHostHandler.getPackageForHost(conn, host));
		if(!userPackage.equals(serverPackage)) throw new SQLException("userPackage!=serverPackage: may only set backup.FileReplication.max_bit_rate for servers that have the same package as the business_administrator setting the bit rate");

		if(bitRate==null) conn.executeUpdate("update backup.\"FileReplication\" set max_bit_rate=null where id=?", fileReplication);
		else conn.executeUpdate("update backup.\"FileReplication\" set max_bit_rate=? where id=?", bitRate, fileReplication);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.FAILOVER_FILE_REPLICATIONS,
			NetHostHandler.getAccountsForHost(conn, host),
			host,
			false
		);
	}

	public static void setFileReplicationSchedules(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int fileReplication,
		List<Short> hours,
		List<Short> minutes
	) throws IOException, SQLException {
		// The server must be an exact package match to allow setting the schedule
		int host = getFromHostForFileReplication(conn, fileReplication);
		Account.Name userPackage = AccountUserHandler.getPackageForUser(conn, source.getCurrentAdministrator());
		Account.Name serverPackage = PackageHandler.getNameForPackage(conn, NetHostHandler.getPackageForHost(conn, host));
		if(!userPackage.equals(serverPackage)) throw new SQLException("userPackage!=serverPackage: may only set backup.FileReplicationSchedule for servers that have the same package as the business_administrator setting the schedule");

		// If not modified, invalidation will not be performed
		boolean modified = false;

		// Get the list of all the ids that currently exist
		IntList ids = conn.executeIntListQuery("select id from backup.\"FileReplicationSchedule\" where replication=?", fileReplication);
		int size = hours.size();
		for(int c=0;c<size;c++) {
			// If it exists, remove id from the list, otherwise add
			short hour = hours.get(c);
			short minute = minutes.get(c);
			int existingPkey = conn.executeIntQuery(
				"select coalesce((select id from backup.\"FileReplicationSchedule\" where replication=? and hour=? and minute=?), -1)",
				fileReplication,
				hour,
				minute
			);
			if(existingPkey==-1) {
				// Doesn't exist, add
				conn.executeUpdate("insert into backup.\"FileReplicationSchedule\" (replication, hour, minute, enabled) values(?,?,?,true)", fileReplication, hour, minute);
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
				NetHostHandler.getAccountsForHost(conn, host),
				host,
				false
			);
		}
	}

	public static void setFileReplicationSettings(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int fileReplication,
		List<String> paths,
		List<Boolean> backupEnableds,
		List<Boolean> requireds
	) throws IOException, SQLException {
		// The server must be an exact package match to allow setting the schedule
		int host = getFromHostForFileReplication(conn, fileReplication);
		Account.Name userPackage = AccountUserHandler.getPackageForUser(conn, source.getCurrentAdministrator());
		Account.Name serverPackage = PackageHandler.getNameForPackage(conn, NetHostHandler.getPackageForHost(conn, host));
		if(!userPackage.equals(serverPackage)) throw new SQLException("userPackage!=serverPackage: may only set backup.FileReplicationSetting for servers that have the same package as the business_administrator making the settings");

		// If not modified, invalidation will not be performed
		boolean modified = false;

		// Get the list of all the ids that currently exist
		IntList ids = conn.executeIntListQuery("select id from backup.\"FileReplicationSetting\" where replication=?", fileReplication);
		int size = paths.size();
		for(int c=0;c<size;c++) {
			// If it exists, remove id from the list, otherwise add
			String path = paths.get(c);
			boolean backupEnabled = backupEnableds.get(c);
			boolean required = requireds.get(c);
			int existingPkey = conn.executeIntQuery(
				"select coalesce((select id from backup.\"FileReplicationSetting\" where replication=? and path=?), -1)",
				fileReplication,
				path
			);
			if(existingPkey==-1) {
				// Doesn't exist, add
				conn.executeUpdate(
					"insert into backup.\"FileReplicationSetting\" (replication, path, backup_enabled, required) values(?,?,?,?)",
					fileReplication,
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
				NetHostHandler.getAccountsForHost(conn, host),
				host,
				false
			);
		}
	}

	public static int getFromHostForFileReplication(DatabaseConnection conn, int fileReplication) throws IOException, SQLException {
		return conn.executeIntQuery("select server from backup.\"FileReplication\" where id=?", fileReplication);
	}

	public static int getBackupPartitionForFileReplication(DatabaseConnection conn, int fileReplication) throws IOException, SQLException {
		return conn.executeIntQuery("select backup_partition from backup.\"FileReplication\" where id=?", fileReplication);
	}

	public static void getFileReplicationLogs(
		DatabaseConnection conn,
		RequestSource source,
		StreamableOutput out,
		int fileReplication,
		int maxRows
	) throws IOException, SQLException {
		// Check access for the from server
		int fromHost = getFromHostForFileReplication(conn, fileReplication);
		NetHostHandler.checkAccessHost(conn, source, "getFileReplicationLogs", fromHost);

		// TODO: release conn before writing to out
		MasterServer.writeObjects(
			conn,
			source,
			out,
			false,
			maxRows > CursorMode.AUTO_CURSOR_ABOVE ? CursorMode.FETCH : CursorMode.SELECT,
			new FileReplicationLog(),
			"select * from backup.\"FileReplicationLog\" where replication=? order by start_time desc limit ?",
			fileReplication,
			maxRows
		);
	}

	public static Tuple2<Long,String> getFileReplicationActivity(
		DatabaseConnection conn,
		RequestSource source,
		int fileReplication
	) throws IOException, SQLException {
		// Check access for the from server
		int fromHost = getFromHostForFileReplication(conn, fileReplication);
		NetHostHandler.checkAccessHost(conn, source, "getFileReplicationActivity", fromHost);

		// Find where going
		int backupPartition = FailoverHandler.getBackupPartitionForFileReplication(conn, fileReplication);
		int toLinuxServer = BackupHandler.getLinuxServerForBackupPartition(conn, backupPartition);

		// Contact the server
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, toLinuxServer);
		conn.releaseConnection();
		return daemonConnector.getFailoverFileReplicationActivity(fileReplication);
	}

	/**
	 * Runs at 1:20 am daily.
	 */
	private static final Schedule schedule = (minute, hour, dayOfMonth, month, dayOfWeek, year) -> minute==45 && hour==1;

	@Override
	public Schedule getSchedule() {
		return schedule;
	}

	@Override
	public int getThreadPriority() {
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
	public void run(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
		try {
			MasterDatabase.getDatabase().executeUpdate("delete from backup.\"FileReplicationLog\" where end_time <= (now()-'1 year'::interval)");
		} catch(RuntimeException | IOException | SQLException T) {
			logger.log(Level.SEVERE, null, T);
		}
	}

	public static Server.DaemonAccess requestReplicationDaemonAccess(
		DatabaseConnection conn,
		RequestSource source,
		int fileReplication
	) throws IOException, SQLException {
		// Security checks
		//String username=source.getUsername();
		//User masterUser=MasterServer.getUser(conn, username);
		//if(masterUser==null) throw new SQLException("Only master users allowed to request daemon access.");
		// Sometime later we might restrict certain command codes to certain users

		// Current user must have the same exact package as the from server
		Account.Name userPackage = AccountUserHandler.getPackageForUser(conn, source.getCurrentAdministrator());
		int fromServer=FailoverHandler.getFromHostForFileReplication(conn, fileReplication);
		Account.Name serverPackage = PackageHandler.getNameForPackage(conn, NetHostHandler.getPackageForHost(conn, fromServer));
		if(!userPackage.equals(serverPackage)) throw new SQLException("account.Administrator.username.package!=servers.package.name: Not allowed to request daemon access for FAILOVER_FILE_REPLICATION");
		//ServerHandler.checkAccessServer(conn, source, "requestDaemonAccess", fromServer);

		// The to server must match server
		int backupPartition = FailoverHandler.getBackupPartitionForFileReplication(conn, fileReplication);
		int toServer = BackupHandler.getLinuxServerForBackupPartition(conn, backupPartition);

		// The overall backup path includes both the toPath and the server name
		String serverName;
		if(NetHostHandler.isLinuxServer(conn, fromServer)) {
			serverName = NetHostHandler.getHostnameForLinuxServer(conn, fromServer).toString();
		} else {
			serverName =
				PackageHandler.getNameForPackage(conn, NetHostHandler.getPackageForHost(conn, fromServer))
				+"/"
				+ NetHostHandler.getNameForHost(conn, fromServer)
			;
		}

		int quota_gid = conn.executeIntQuery("select coalesce(quota_gid, -1) from backup.\"FileReplication\" where id=?", fileReplication);

		// Verify that the backup_partition is the correct type
		boolean isQuotaEnabled = conn.executeBooleanQuery("select bp.quota_enabled from backup.\"FileReplication\" ffr inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where ffr.id=?", fileReplication);
		if(quota_gid==-1) {
			if(isQuotaEnabled) throw new SQLException("quota_gid is null when quota_enabled=true: backup.FileReplication.id="+fileReplication);
		} else {
			if(!isQuotaEnabled) throw new SQLException("quota_gid is not null when quota_enabled=false: backup.FileReplication.id="+fileReplication);
		}

		HostAddress connectAddress = conn.executeObjectQuery(
			ObjectFactories.hostAddressFactory,
			"select connect_address from backup.\"FileReplication\" where id=?",
			fileReplication
		);
		return DaemonHandler.grantDaemonAccess(
			conn,
			toServer,
			connectAddress,
			AOServDaemonProtocol.FAILOVER_FILE_REPLICATION,
			Integer.toString(fileReplication),
			serverName,
			conn.executeStringQuery("select bp.path from backup.\"FileReplication\" ffr inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where ffr.id=?", fileReplication),
			quota_gid==-1 ? null : Integer.toString(quota_gid)
		);
	}
}
