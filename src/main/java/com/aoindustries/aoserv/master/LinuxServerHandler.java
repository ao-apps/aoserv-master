/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2003-2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022  AO Industries, Inc.
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
package com.aoindustries.aoserv.master;

import com.aoapps.collections.IntList;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoapps.net.InetAddress;
import com.aoapps.net.Port;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * The <code>LinuxServerHandler</code> handles all the accesses to the linux.Server table.
 *
 * @author  AO Industries, Inc.
 */
public abstract class LinuxServerHandler {

	/** Make no instances. */
	private LinuxServerHandler() {throw new AssertionError();}

	public static IntList getServers(DatabaseConnection conn) throws SQLException {
		return conn.queryIntList("select server from linux.\"Server\"");
	}

	public static int getUidMin(DatabaseConnection conn, int linuxServer) throws SQLException {
		return conn.queryInt("select \"uidMin\" from linux.\"Server\" where server=?", linuxServer);
	}

	public static int getUidMax(DatabaseConnection conn, int linuxServer) throws SQLException {
		return conn.queryInt("select \"uidMax\" from linux.\"Server\" where server=?", linuxServer);
	}

	public static int getGidMin(DatabaseConnection conn, int linuxServer) throws SQLException {
		return conn.queryInt("select \"gidMin\" from linux.\"Server\" where server=?", linuxServer);
	}

	public static int getGidMax(DatabaseConnection conn, int linuxServer) throws SQLException {
		return conn.queryInt("select \"gidMax\" from linux.\"Server\" where server=?", linuxServer);
	}

	private static final Map<Integer, Object> mrtgLocks = new HashMap<>();

	public static void getMrtgFile(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer,
		String filename,
		StreamableOutput out
	) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "getMrtgFile", linuxServer);
		if(filename.indexOf('/')!=-1 || filename.contains("..")) throw new SQLException("Invalidate filename: "+filename);

		// Only one MRTG graph per server at a time, if don't get the lock in 15 seconds, return an error
		synchronized(mrtgLocks) {
			long startTime = System.currentTimeMillis();
			do {
				if(mrtgLocks.containsKey(linuxServer)) {
					long currentTime = System.currentTimeMillis();
					if(startTime > currentTime) startTime = currentTime;
					else if((currentTime - startTime)>=15000) throw new IOException("15 second timeout reached while trying to get lock to access server #"+linuxServer);
					else {
						try {
							mrtgLocks.wait(startTime + 15000 - currentTime);
						} catch(InterruptedException err) {
							// Restore the interrupted status
							Thread.currentThread().interrupt();
							InterruptedIOException ioErr = new InterruptedIOException();
							ioErr.initCause(err);
							throw ioErr;
						}
					}
				}
			} while(mrtgLocks.containsKey(linuxServer));
			mrtgLocks.put(linuxServer, Boolean.TRUE);
			mrtgLocks.notifyAll();
		}
		try {
			if(DaemonHandler.isDaemonAvailable(linuxServer)) {
				try {
					AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
					conn.close(); // Don't hold database connection while connecting to the daemon
					daemonConnector.getMrtgFile(filename, out);
				} catch(IOException err) {
					DaemonHandler.flagDaemonAsDown(linuxServer);
					throw new IOException("Host Unavailable", err);
				}
			} else throw new IOException("Host Unavailable");
		} finally {
			synchronized(mrtgLocks) {
				mrtgLocks.remove(linuxServer);
				mrtgLocks.notifyAll();
			}
		}
	}

	public static void setLastDistroTime(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int linuxServer,
		Timestamp time
	) throws IOException, SQLException {
		com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
		User mu = MasterServer.getUser(conn, currentAdministrator);
		if (mu==null) throw new SQLException("User "+currentAdministrator+" is not master user and may not set the last distro time");
		NetHostHandler.checkAccessHost(conn, source, "setLastDistroTime", linuxServer);
		conn.update(
			"update linux.\"Server\" set last_distro_time=? where server=?",
			time,
			linuxServer
		);
		invalidateList.addTable(
			conn,
			Table.TableID.SERVERS,
			NetHostHandler.getAccountsForHost(conn, linuxServer),
			linuxServer,
			false
		);
	}

	public static void startDistro(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer,
		boolean includeUser
	) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "startDistro", linuxServer);
		User mu=MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu==null) throw new SQLException("Only master users may start distribution verifications: "+source.getCurrentAdministrator());
		NetHostHandler.checkAccessHost(conn, source, "startDistro", linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		daemonConnector.startDistro(includeUser);
	}

	public static void restartCron(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer
	) throws IOException, SQLException {
		boolean canControl=AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_cron");
		if(!canControl) throw new SQLException("Not allowed to restart Cron on "+linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		daemonConnector.restartCron();
	}

	public static void startCron(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer
	) throws IOException, SQLException {
		boolean canControl=AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_cron");
		if(!canControl) throw new SQLException("Not allowed to start Cron on "+linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		daemonConnector.startCron();
	}

	public static void stopCron(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer
	) throws IOException, SQLException {
		boolean canControl=AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_cron");
		if(!canControl) throw new SQLException("Not allowed to stop Cron on "+linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		daemonConnector.stopCron();
	}

	public static void restartXfs(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer
	) throws IOException, SQLException {
		boolean canControl=AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_xfs");
		if(!canControl) throw new SQLException("Not allowed to restart XFS on "+linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		daemonConnector.restartXfs();
	}

	public static void startXfs(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer
	) throws IOException, SQLException {
		boolean canControl=AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_xfs");
		if(!canControl) throw new SQLException("Not allowed to start XFS on "+linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		daemonConnector.startXfs();
	}

	public static void stopXfs(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer
	) throws IOException, SQLException {
		boolean canControl=AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_xfs");
		if(!canControl) throw new SQLException("Not allowed to stop XFS on "+linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		daemonConnector.stopXfs();
	}

	public static void restartXvfb(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer
	) throws IOException, SQLException {
		boolean canControl=AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_xvfb");
		if(!canControl) throw new SQLException("Not allowed to restart Xvfb on "+linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		daemonConnector.restartXvfb();
	}

	public static void startXvfb(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer
	) throws IOException, SQLException {
		boolean canControl=AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_xvfb");
		if(!canControl) throw new SQLException("Not allowed to start Xvfb on "+linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		daemonConnector.startXvfb();
	}

	public static void stopXvfb(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer
	) throws IOException, SQLException {
		boolean canControl=AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_xvfb");
		if(!canControl) throw new SQLException("Not allowed to stop Xvfb on "+linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		daemonConnector.stopXvfb();
	}

	public static String get3wareRaidReport(DatabaseConnection conn, RequestSource source, int linuxServer) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "get3wareRaidReport", linuxServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.get3wareRaidReport();
	}

	public static String getUpsStatus(DatabaseConnection conn, RequestSource source, int linuxServer) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "getUpsStatus", linuxServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.getUpsStatus();
	}

	public static String getMdStatReport(DatabaseConnection conn, RequestSource source, int linuxServer) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "getMdStatReport", linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.getMdStatReport();
	}

	public static String getMdMismatchReport(DatabaseConnection conn, RequestSource source, int linuxServer) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "getMdMismatchReport", linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.getMdMismatchReport();
	}

	public static String getDrbdReport(DatabaseConnection conn, RequestSource source, int linuxServer) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "getDrbdReport", linuxServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.getDrbdReport();
	}

	public static String[] getLvmReport(DatabaseConnection conn, RequestSource source, int linuxServer) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "getLvmReport", linuxServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.getLvmReport();
	}

	public static String getHddTempReport(DatabaseConnection conn, RequestSource source, int linuxServer) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "getHddTempReport", linuxServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.getHddTempReport();
	}

	public static String getHddModelReport(DatabaseConnection conn, RequestSource source, int linuxServer) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "getHddModelReport", linuxServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.getHddModelReport();
	}

	public static String getFilesystemsCsvReport(DatabaseConnection conn, RequestSource source, int linuxServer) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "getFilesystemsCsvReport", linuxServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.getFilesystemsCsvReport();
	}

	public static String getLoadAvgReport(DatabaseConnection conn, RequestSource source, int linuxServer) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "getLoadAvgReport", linuxServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.getLoadAvgReport();
	}

	public static String getMemInfoReport(DatabaseConnection conn, RequestSource source, int linuxServer) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "getMemInfoReport", linuxServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.getMemInfoReport();
	}

	public static String checkPort(DatabaseConnection conn, RequestSource source, int linuxServer, InetAddress ipAddress, Port port, String appProtocol, String monitoringParameters) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "checkPort", linuxServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.checkPort(ipAddress, port, appProtocol, monitoringParameters);
	}

	public static String checkSmtpBlacklist(DatabaseConnection conn, RequestSource source, int linuxServer, InetAddress sourceIp, InetAddress connectIp) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "checkSmtpBlacklist", linuxServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.checkSmtpBlacklist(sourceIp, connectIp);
	}

	public static long getSystemTimeMillis(DatabaseConnection conn, RequestSource source, int linuxServer) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "getSystemTimeMillis", linuxServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.getSystemTimeMillis();
	}
}
