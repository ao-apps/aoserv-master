/*
 * Copyright 2003-2013, 2014, 2015, 2016, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.net.InetAddress;
import com.aoindustries.net.Port;
import com.aoindustries.util.IntList;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * The <code>AOServerHandler</code> handles all the accesses to the linux.Server table.
 *
 * @author  AO Industries, Inc.
 */
final public class AOServerHandler {

	private static final Logger logger = LogFactory.getLogger(AOServerHandler.class);

	private AOServerHandler() {
	}

	public static IntList getAOServers(DatabaseConnection conn) throws SQLException {
		return conn.executeIntListQuery("select server from linux.\"Server\"");
	}

	public static int getUidMin(DatabaseConnection conn, int aoServer) throws SQLException {
		return conn.executeIntQuery("select \"uidMin\" from linux.\"Server\" where server=?", aoServer);
	}

	public static int getUidMax(DatabaseConnection conn, int aoServer) throws SQLException {
		return conn.executeIntQuery("select \"uidMax\" from linux.\"Server\" where server=?", aoServer);
	}

	public static int getGidMin(DatabaseConnection conn, int aoServer) throws SQLException {
		return conn.executeIntQuery("select \"gidMin\" from linux.\"Server\" where server=?", aoServer);
	}

	public static int getGidMax(DatabaseConnection conn, int aoServer) throws SQLException {
		return conn.executeIntQuery("select \"gidMax\" from linux.\"Server\" where server=?", aoServer);
	}

	private static final Map<Integer,Object> mrtgLocks = new HashMap<>();

	public static void getMrtgFile(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer,
		String filename,
		CompressedDataOutputStream out
	) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "getMrtgFile", aoServer);
		if(filename.indexOf('/')!=-1 || filename.contains("..")) throw new SQLException("Invalidate filename: "+filename);

		// Only one MRTG graph per server at a time, if don't get the lock in 15 seconds, return an error
		synchronized(mrtgLocks) {
			long startTime = System.currentTimeMillis();
			do {
				if(mrtgLocks.containsKey(aoServer)) {
					long currentTime = System.currentTimeMillis();
					if(startTime > currentTime) startTime = currentTime;
					else if((currentTime - startTime)>=15000) throw new IOException("15 second timeout reached while trying to get lock to access server #"+aoServer);
					else {
						try {
							mrtgLocks.wait(startTime + 15000 - currentTime);
						} catch(InterruptedException err) {
							IOException ioErr = new InterruptedIOException();
							ioErr.initCause(err);
							throw ioErr;
						}
					}
				}
			} while(mrtgLocks.containsKey(aoServer));
			mrtgLocks.put(aoServer, Boolean.TRUE);
			mrtgLocks.notifyAll();
		}
		try {
			if(DaemonHandler.isDaemonAvailable(aoServer)) {
				try {
					AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
					conn.releaseConnection();
					daemonConnector.getMrtgFile(filename, out);
				} catch(IOException err) {
					DaemonHandler.flagDaemonAsDown(aoServer);
					throw new IOException("Host Unavailable", err);
				}
			} else throw new IOException("Host Unavailable");
		} finally {
			synchronized(mrtgLocks) {
				mrtgLocks.remove(aoServer);
				mrtgLocks.notifyAll();
			}
		}
	}

	public static void setLastDistroTime(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int aoServer,
		long time
	) throws IOException, SQLException {
		com.aoindustries.aoserv.client.account.User.Name mustring = source.getUsername();
		User mu = MasterServer.getUser(conn, mustring);
		if (mu==null) throw new SQLException("User "+mustring+" is not master user and may not set the last distro time");
		ServerHandler.checkAccessServer(conn, source, "setLastDistroTime", aoServer);
		conn.executeUpdate(
			"update linux.\"Server\" set last_distro_time=? where server=?",
			new Timestamp(time),
			aoServer
		);
		invalidateList.addTable(
			conn,
			Table.TableID.SERVERS,
			ServerHandler.getBusinessesForServer(conn, aoServer),
			aoServer,
			false
		);
	}

	public static void startDistro(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer,
		boolean includeUser
	) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "startDistro", aoServer);
		User mu=MasterServer.getUser(conn, source.getUsername());
		if(mu==null) throw new SQLException("Only master users may start distribution verifications: "+source.getUsername());
		ServerHandler.checkAccessServer(conn, source, "startDistro", aoServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		daemonConnector.startDistro(includeUser);
	}

	public static void restartCron(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_cron");
		if(!canControl) throw new SQLException("Not allowed to restart Cron on "+aoServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		daemonConnector.restartCron();
	}

	public static void startCron(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_cron");
		if(!canControl) throw new SQLException("Not allowed to start Cron on "+aoServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		daemonConnector.startCron();
	}

	public static void stopCron(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_cron");
		if(!canControl) throw new SQLException("Not allowed to stop Cron on "+aoServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		daemonConnector.stopCron();
	}

	public static void restartXfs(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_xfs");
		if(!canControl) throw new SQLException("Not allowed to restart XFS on "+aoServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		daemonConnector.restartXfs();
	}

	public static void startXfs(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_xfs");
		if(!canControl) throw new SQLException("Not allowed to start XFS on "+aoServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		daemonConnector.startXfs();
	}

	public static void stopXfs(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_xfs");
		if(!canControl) throw new SQLException("Not allowed to stop XFS on "+aoServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		daemonConnector.stopXfs();
	}

	public static void restartXvfb(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_xvfb");
		if(!canControl) throw new SQLException("Not allowed to restart Xvfb on "+aoServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		daemonConnector.restartXvfb();
	}

	public static void startXvfb(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_xvfb");
		if(!canControl) throw new SQLException("Not allowed to start Xvfb on "+aoServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		daemonConnector.startXvfb();
	}

	public static void stopXvfb(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_xvfb");
		if(!canControl) throw new SQLException("Not allowed to stop Xvfb on "+aoServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		daemonConnector.stopXvfb();
	}

	public static String get3wareRaidReport(DatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "get3wareRaidReport", aoServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		return daemonConnector.get3wareRaidReport();
	}

	public static String getUpsStatus(DatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "getUpsStatus", aoServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		return daemonConnector.getUpsStatus();
	}

	public static String getMdStatReport(DatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "getMdStatReport", aoServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		return daemonConnector.getMdStatReport();
	}

	public static String getMdMismatchReport(DatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "getMdMismatchReport", aoServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		return daemonConnector.getMdMismatchReport();
	}

	public static String getDrbdReport(DatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "getDrbdReport", aoServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		return daemonConnector.getDrbdReport();
	}

	public static String[] getLvmReport(DatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "getLvmReport", aoServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		return daemonConnector.getLvmReport();
	}

	public static String getHddTempReport(DatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "getHddTempReport", aoServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		return daemonConnector.getHddTempReport();
	}

	public static String getHddModelReport(DatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "getHddModelReport", aoServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		return daemonConnector.getHddModelReport();
	}

	public static String getFilesystemsCsvReport(DatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "getFilesystemsCsvReport", aoServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		return daemonConnector.getFilesystemsCsvReport();
	}

	public static String getLoadAvgReport(DatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "getLoadAvgReport", aoServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		return daemonConnector.getLoadAvgReport();
	}

	public static String getMemInfoReport(DatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "getMemInfoReport", aoServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		return daemonConnector.getMemInfoReport();
	}

	public static String checkPort(DatabaseConnection conn, RequestSource source, int aoServer, InetAddress ipAddress, Port port, String appProtocol, String monitoringParameters) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "checkPort", aoServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		return daemonConnector.checkPort(ipAddress, port, appProtocol, monitoringParameters);
	}

	public static String checkSmtpBlacklist(DatabaseConnection conn, RequestSource source, int aoServer, InetAddress sourceIp, InetAddress connectIp) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "checkSmtpBlacklist", aoServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		return daemonConnector.checkSmtpBlacklist(sourceIp, connectIp);
	}

	public static long getSystemTimeMillis(DatabaseConnection conn, RequestSource source, int aoServer) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "getSystemTimeMillis", aoServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		return daemonConnector.getSystemTimeMillis();
	}
}
