/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2002-2013, 2015, 2017, 2018, 2019, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.collections.IntList;
import com.aoapps.dbc.DatabaseConnection;
import com.aoindustries.aoserv.client.distribution.OperatingSystemVersion;
import com.aoindustries.aoserv.client.linux.PosixPath;
import com.aoindustries.aoserv.client.linux.UserType;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.scm.CvsRepository;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * The <code>CvsHandler</code> handles all the accesses to the <code>scm.CvsRepository</code> table.
 *
 * @author  AO Industries, Inc.
 */
public final class CvsHandler {

	private static final Map<Integer, Boolean> disabledCvsRepositories = new HashMap<>();

	public static int addCvsRepository(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int linuxServer, // TODO: Redundant with linuxUserServer and linuxGroupServer
		PosixPath path,
		int linuxUserServer,
		int linuxGroupServer,
		long mode
	) throws IOException, SQLException {
		synchronized(CvsHandler.class) {
			// Security checks
			NetHostHandler.checkAccessHost(conn, source, "addCvsRepository", linuxServer);
			LinuxAccountHandler.checkAccessUserServer(conn, source, "addCvsRepository", linuxUserServer);
			LinuxAccountHandler.checkAccessGroupServer(conn, source, "addCvsRepository", linuxGroupServer);
			if(LinuxAccountHandler.isUserServerDisabled(conn, linuxUserServer)) throw new SQLException("Unable to add CvsRepository, UserServer disabled: "+linuxUserServer);

			// OperatingSystem settings
			int osv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
			PosixPath httpdSharedTomcatsDir = OperatingSystemVersion.getHttpdSharedTomcatsDirectory(osv);
			String httpdSharedTomcatsDirStr = httpdSharedTomcatsDir.toString();
			PosixPath httpdSitesDir = OperatingSystemVersion.getHttpdSitesDirectory(osv);
			String httpdSitesDirStr = httpdSitesDir.toString();

			// Integrity checks
			if(!CvsRepository.isValidPath(path)) throw new SQLException("Invalid path: "+path);
			String pathStr = path.toString();
			if(pathStr.startsWith("/home/")) {
				// Must be able to access one of the linux.UserServer with that home directory
				//
				// This means there must be an accessible account that has a home directory that is a prefix of this docbase.
				// Such as /home/e/example/ being a prefix of /home/e/example/my-webapp
				IntList lsas = conn.queryIntList(
					"select\n"
					+ "  id\n"
					+ "from\n"
					+ "  linux.\"UserServer\"\n"
					+ "where\n"
					+ "  ao_server=?\n"
					+ "  and (home || '/')=substring(? from 1 for (length(home) + 1))",
					linuxServer,
					pathStr
				);
				boolean found=false;
				for(int c=0;c<lsas.size();c++) {
					if(LinuxAccountHandler.canAccessUserServer(conn, source, lsas.getInt(c))) {
						found=true;
						break;
					}
				}
				if(!found) throw new SQLException("Home directory not allowed for path: " + pathStr);
			} else if(pathStr.startsWith(CvsRepository.DEFAULT_CVS_DIRECTORY + "/")) {
				// Must be directly in /var/cvs/ folder.
				int slashPos=pathStr.indexOf('/', CvsRepository.DEFAULT_CVS_DIRECTORY.toString().length() + 1);
				if(slashPos!=-1) throw new SQLException("Invalid path: "+path);
			} else if(pathStr.startsWith(httpdSitesDirStr+ '/')) {
				int slashPos = pathStr.indexOf('/', httpdSitesDirStr.length() + 1);
				if(slashPos == -1) slashPos = pathStr.length();
				String siteName = pathStr.substring(httpdSitesDirStr.length() + 1, slashPos);
				int hs = conn.queryInt("select id from web.\"Site\" where ao_server=? and \"name\"=?", linuxServer, siteName);
				WebHandler.checkAccessSite(conn, source, "addCvsRepository", hs);
			} else if(pathStr.startsWith(httpdSharedTomcatsDirStr + '/')) {
				int slashPos = pathStr.indexOf('/', httpdSharedTomcatsDirStr.length() + 1);
				if(slashPos == -1) slashPos = pathStr.length();
				String groupName = pathStr.substring(httpdSharedTomcatsDirStr.length() + 1, slashPos);
				int groupLSA = conn.queryInt("select linux_server_account from \"web.tomcat\".\"SharedTomcat\" where name=? and ao_server=?", groupName, linuxServer);
				LinuxAccountHandler.checkAccessUserServer(conn, source, "addCvsRepository", groupLSA);
			} else {
				throw new SQLException("Invalid path: " + path);
			}

			// Must not already have an existing CVS repository on this server
			if(
				conn.queryBoolean(
					"select\n"
					+ "  (\n"
					+ "    select\n"
					+ "      cr.id\n"
					+ "    from\n"
					+ "      scm.\"CvsRepository\" cr,\n"
					+ "      linux.\"UserServer\" lsa\n"
					+ "    where\n"
					+ "      cr.path=?\n"
					+ "      and cr.linux_server_account=lsa.id\n"
					+ "      and lsa.ao_server=?\n"
					+ "    limit 1\n"
					+ "  ) is not null",
					path,
					linuxServer
				)
			) throw new SQLException("CvsRepository already exists: "+path+" on Server #"+linuxServer);

			int userServer_linuxServer = LinuxAccountHandler.getServerForUserServer(conn, linuxUserServer);
			if(userServer_linuxServer != linuxServer) throw new SQLException("linux.UserServer "+linuxUserServer+" is not located on Server #"+linuxServer);
			String type=LinuxAccountHandler.getTypeForUserServer(conn, linuxUserServer);
			if(
				!(
					UserType.USER.equals(type)
					|| UserType.APPLICATION.equals(type)
				)
			) throw new SQLException("CVS repositories must be owned by a linux account of type '"+UserType.USER+"' or '"+UserType.APPLICATION+'\'');

			int groupServer_linuxServer = LinuxAccountHandler.getServerForGroupServer(conn, linuxGroupServer);
			if(groupServer_linuxServer != linuxServer) throw new SQLException("linux.GroupServer "+linuxGroupServer+" is not located on Server #"+linuxServer);

			long[] modes=CvsRepository.getValidModes();
			boolean found=false;
			for(int c=0;c<modes.length;c++) {
				if(modes[c]==mode) {
				found=true;
				break;
				}
			}
			if(!found) throw new SQLException("Invalid mode: "+mode);

			// Update the database
			int cvsRepository = conn.updateInt(
				"INSERT INTO scm.\"CvsRepository\" (\n"
				+ "  \"path\",\n"
				+ "  linux_server_account,\n"
				+ "  linux_server_group,\n"
				+ "  mode\n"
				+ ") VALUES (\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?\n"
				+ ") RETURNING id",
				path,
				linuxUserServer,
				linuxGroupServer,
				mode
			);
			invalidateList.addTable(
				conn,
				Table.TableID.CVS_REPOSITORIES,
				LinuxAccountHandler.getAccountForUserServer(conn, linuxUserServer),
				linuxServer,
				false
			);
			return cvsRepository;
		}
	}

	/**
	 * Disables a CVS repository.
	 */
	public static void disableCvsRepository(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		int cvsRepository
	) throws IOException, SQLException {
		AccountHandler.checkAccessDisableLog(conn, source, "disableCvsRepository", disableLog, false);
		int lsa=getLinuxUserServerForCvsRepository(conn, cvsRepository);
		LinuxAccountHandler.checkAccessUserServer(conn, source, "disableCvsRepository", lsa);
		if(isCvsRepositoryDisabled(conn, cvsRepository)) throw new SQLException("CvsRepository is already disabled: "+cvsRepository);

		conn.update(
			"update scm.\"CvsRepository\" set disable_log=? where id=?",
			disableLog,
			cvsRepository
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.CVS_REPOSITORIES,
			LinuxAccountHandler.getAccountForUserServer(conn, lsa),
			LinuxAccountHandler.getServerForUserServer(conn, lsa),
			false
		);
	}

	public static void enableCvsRepository(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int cvsRepository
	) throws IOException, SQLException {
		int lsa = getLinuxUserServerForCvsRepository(conn, cvsRepository);
		LinuxAccountHandler.checkAccessUserServer(conn, source, "enableCvsRepository", lsa);
		int disableLog=getDisableLogForCvsRepository(conn, cvsRepository);
		if(disableLog==-1) throw new SQLException("CvsRepository is already enabled: "+cvsRepository);
		AccountHandler.checkAccessDisableLog(conn, source, "enableCvsRepository", disableLog, true);
		if(LinuxAccountHandler.isUserServerDisabled(conn, lsa)) throw new SQLException("Unable to enable CvsRepository #"+cvsRepository+", UserServer not enabled: "+lsa);

		conn.update(
			"update scm.\"CvsRepository\" set disable_log=null where id=?",
			cvsRepository
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.CVS_REPOSITORIES,
			LinuxAccountHandler.getAccountForUserServer(conn, lsa),
			LinuxAccountHandler.getServerForUserServer(conn, lsa),
			false
		);
	}

	public static IntList getCvsRepositoriesForLinuxUserServer(DatabaseConnection conn, int linuxUserServer) throws IOException, SQLException {
		return conn.queryIntList("select id from scm.\"CvsRepository\" where linux_server_account=?", linuxUserServer);
	}

	public static int getLinuxUserServerForCvsRepository(DatabaseConnection conn, int cvsRepository) throws IOException, SQLException {
		return conn.queryInt("select linux_server_account from scm.\"CvsRepository\" where id=?", cvsRepository);
	}

	public static void invalidateTable(Table.TableID tableID) {
		if(tableID==Table.TableID.CVS_REPOSITORIES) {
			synchronized(CvsHandler.class) {
				disabledCvsRepositories.clear();
			}
		}
	}

	public static int getDisableLogForCvsRepository(DatabaseConnection conn, int cvsRepository) throws IOException, SQLException {
		return conn.queryInt("select coalesce(disable_log, -1) from scm.\"CvsRepository\" where id=?", cvsRepository);
	}

	public static boolean isCvsRepositoryDisabled(DatabaseConnection conn, int cvsRepository) throws IOException, SQLException {
		Integer idObj = cvsRepository;
		synchronized(CvsHandler.class) {
			Boolean isDisabledObj = disabledCvsRepositories.get(idObj);
			if(isDisabledObj != null) return isDisabledObj;
			boolean isDisabled = getDisableLogForCvsRepository(conn, cvsRepository) != -1;
			disabledCvsRepositories.put(idObj, isDisabled);
			return isDisabled;
		}
	}

	public static void removeCvsRepository(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int cvsRepository
	) throws IOException, SQLException {
		// Security checks
		int lsa=getLinuxUserServerForCvsRepository(conn, cvsRepository);
		LinuxAccountHandler.checkAccessUserServer(conn, source, "removeCvsRepository", lsa);

		removeCvsRepository(conn, invalidateList, cvsRepository);
	}

	public static void removeCvsRepository(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int cvsRepository
	) throws IOException, SQLException {
		// Grab values for later use
		int linuxUserServer = getLinuxUserServerForCvsRepository(conn, cvsRepository);
		int linuxServer = LinuxAccountHandler.getServerForUserServer(conn, linuxUserServer);

		// Update the database
		conn.update("delete from scm.\"CvsRepository\" where id=?", cvsRepository);

		invalidateList.addTable(
			conn,
			Table.TableID.CVS_REPOSITORIES,
			LinuxAccountHandler.getAccountForUserServer(conn, linuxUserServer),
			linuxServer,
			false
		);
	}

	public static void setMode(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int cvsRepository,
		long mode
	) throws IOException, SQLException {
		// Security checks
		int lsa=getLinuxUserServerForCvsRepository(conn, cvsRepository);
		LinuxAccountHandler.checkAccessUserServer(conn, source, "setMode", lsa);

		// Integrity checks
		long[] modes=CvsRepository.getValidModes();
		boolean found=false;
		for(int c=0;c<modes.length;c++) {
			if(modes[c]==mode) {
				found=true;
				break;
			}
		}
		if(!found) throw new SQLException("Invalid mode: "+mode);

		// Update the database
		conn.update(
			"update scm.\"CvsRepository\" set mode=? where id=?",
			mode,
			cvsRepository
		);

		invalidateList.addTable(
			conn,
			Table.TableID.CVS_REPOSITORIES,
			LinuxAccountHandler.getAccountForUserServer(conn, lsa),
			LinuxAccountHandler.getServerForUserServer(conn, lsa),
			false
		);
	}

	/**
	 * Make no instances.
	 */
	private CvsHandler() {
	}
}
