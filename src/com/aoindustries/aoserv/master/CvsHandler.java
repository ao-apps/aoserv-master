/*
 * Copyright 2002-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.CvsRepository;
import com.aoindustries.aoserv.client.LinuxAccountType;
import com.aoindustries.aoserv.client.OperatingSystemVersion;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.validator.UnixPath;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.util.IntList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * The <code>CvsHandler</code> handles all the accesses to the <code>cvs_repositories</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class CvsHandler {

	private final static Map<Integer,Boolean> disabledCvsRepositories=new HashMap<>();

	public static int addCvsRepository(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int aoServer,
		UnixPath path,
		int lsa,
		int lsg,
		long mode
	) throws IOException, SQLException {
		synchronized(CvsHandler.class) {
			// Security checks
			ServerHandler.checkAccessServer(conn, source, "addCvsRepository", aoServer);
			LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "addCvsRepository", lsa);
			LinuxAccountHandler.checkAccessLinuxServerGroup(conn, source, "addCvsRepository", lsg);
			if(LinuxAccountHandler.isLinuxServerAccountDisabled(conn, lsa)) throw new SQLException("Unable to add CvsRepository, LinuxServerAccount disabled: "+lsa);

			// OperatingSystem settings
			int osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
			UnixPath httpdSharedTomcatsDir = OperatingSystemVersion.getHttpdSharedTomcatsDirectory(osv);
			String httpdSharedTomcatsDirStr = httpdSharedTomcatsDir.toString();
			UnixPath httpdSitesDir = OperatingSystemVersion.getHttpdSitesDirectory(osv);
			String httpdSitesDirStr = httpdSitesDir.toString();

			// Integrity checks
			if(!CvsRepository.isValidPath(path)) throw new SQLException("Invalid path: "+path);
			String pathStr = path.toString();
			if(pathStr.startsWith("/home/")) {
				// Must be able to access one of the linux_server_accounts with that home directory
				//
				// This means there must be an accessible account that has a home directory that is a prefix of this docbase.
				// Such as /home/e/example/ being a prefix of /home/e/example/my-webapp
				IntList lsas = conn.executeIntListQuery(
					"select\n"
					+ "  pkey\n"
					+ "from\n"
					+ "  linux_server_accounts\n"
					+ "where\n"
					+ "  ao_server=?\n"
					+ "  and (home || '/')=substring(? from 1 for (length(home) + 1))",
					aoServer,
					pathStr
				);
				boolean found=false;
				for(int c=0;c<lsas.size();c++) {
					if(LinuxAccountHandler.canAccessLinuxServerAccount(conn, source, lsas.getInt(c))) {
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
				int hs = conn.executeIntQuery("select pkey from httpd_sites where ao_server=? and \"name\"=?", aoServer, siteName);
				HttpdHandler.checkAccessHttpdSite(conn, source, "addCvsRepository", hs);
			} else if(pathStr.startsWith(httpdSharedTomcatsDirStr + '/')) {
				int slashPos = pathStr.indexOf('/', httpdSharedTomcatsDirStr.length() + 1);
				if(slashPos == -1) slashPos = pathStr.length();
				String groupName = pathStr.substring(httpdSharedTomcatsDirStr.length() + 1, slashPos);
				int groupLSA = conn.executeIntQuery("select linux_server_account from httpd_shared_tomcats where name=? and ao_server=?", groupName, aoServer);
				LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "addCvsRepository", groupLSA);
			} else {
				throw new SQLException("Invalid path: " + path);
			}

			// Must not already have an existing CVS repository on this server
			if(
				conn.executeBooleanQuery(
					"select\n"
					+ "  (\n"
					+ "    select\n"
					+ "      cr.pkey\n"
					+ "    from\n"
					+ "      cvs_repositories cr,\n"
					+ "      linux_server_accounts lsa\n"
					+ "    where\n"
					+ "      cr.path=?\n"
					+ "      and cr.linux_server_account=lsa.pkey\n"
					+ "      and lsa.ao_server=?\n"
					+ "    limit 1\n"
					+ "  ) is not null",
					path,
					aoServer
				)
			) throw new SQLException("CvsRepository already exists: "+path+" on AOServer #"+aoServer);

			int lsaAOServer=LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, lsa);
			if(lsaAOServer!=aoServer) throw new SQLException("LinuxServerAccount "+lsa+" is not located on AOServer #"+aoServer);
			String type=LinuxAccountHandler.getTypeForLinuxServerAccount(conn, lsa);
			if(
				!(
					LinuxAccountType.USER.equals(type)
					|| LinuxAccountType.APPLICATION.equals(type)
				)
			) throw new SQLException("CVS repositories must be owned by a linux account of type '"+LinuxAccountType.USER+"' or '"+LinuxAccountType.APPLICATION+'\'');

			int lsgAOServer=LinuxAccountHandler.getAOServerForLinuxServerGroup(conn, lsg);
			if(lsgAOServer!=aoServer) throw new SQLException("LinuxServerGroup "+lsg+" is not located on AOServer #"+aoServer);

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
			int pkey = conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('cvs_repositories_pkey_seq')");
			conn.executeUpdate(
				"insert into\n"
				+ "  cvs_repositories\n"
				+ "values(\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  now(),\n"
				+ "  null\n"
				+ ")",
				pkey,
				path,
				lsa,
				lsg,
				mode
			);
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.CVS_REPOSITORIES,
				LinuxAccountHandler.getBusinessForLinuxServerAccount(conn, lsa),
				aoServer,
				false
			);
			return pkey;
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
		int pkey
	) throws IOException, SQLException {
		if(isCvsRepositoryDisabled(conn, pkey)) throw new SQLException("CvsRepository is already disabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "disableCvsRepository", disableLog, false);
		int lsa=getLinuxServerAccountForCvsRepository(conn, pkey);
		LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "disableCvsRepository", lsa);

		conn.executeUpdate(
			"update cvs_repositories set disable_log=? where pkey=?",
			disableLog,
			pkey
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.CVS_REPOSITORIES,
			LinuxAccountHandler.getBusinessForLinuxServerAccount(conn, lsa),
			LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, lsa),
			false
		);
	}

	public static void enableCvsRepository(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int disableLog=getDisableLogForCvsRepository(conn, pkey);
		if(disableLog==-1) throw new SQLException("CvsRepository is already enabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "enableCvsRepository", disableLog, true);
		int lsa=getLinuxServerAccountForCvsRepository(conn, pkey);
		LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "enableCvsRepository", lsa);
		if(LinuxAccountHandler.isLinuxServerAccountDisabled(conn, lsa)) throw new SQLException("Unable to enable CvsRepository #"+pkey+", LinuxServerAccount not enabled: "+lsa);

		conn.executeUpdate(
			"update cvs_repositories set disable_log=null where pkey=?",
			pkey
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.CVS_REPOSITORIES,
			LinuxAccountHandler.getBusinessForLinuxServerAccount(conn, lsa),
			LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, lsa),
			false
		);
	}

	public static IntList getCvsRepositoriesForLinuxServerAccount(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntListQuery("select pkey from cvs_repositories where linux_server_account=?", pkey);
	}

	public static int getLinuxServerAccountForCvsRepository(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select linux_server_account from cvs_repositories where pkey=?", pkey);
	}

	public static void invalidateTable(SchemaTable.TableID tableID) {
		if(tableID==SchemaTable.TableID.CVS_REPOSITORIES) {
			synchronized(CvsHandler.class) {
				disabledCvsRepositories.clear();
			}
		}
	}

	public static int getDisableLogForCvsRepository(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from cvs_repositories where pkey=?", pkey);
	}

	public static boolean isCvsRepositoryDisabled(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		Integer pkeyObj = pkey;
		synchronized(CvsHandler.class) {
			Boolean isDisabledObj = disabledCvsRepositories.get(pkeyObj);
			if(isDisabledObj != null) return isDisabledObj;
			boolean isDisabled = getDisableLogForCvsRepository(conn, pkey) != -1;
			disabledCvsRepositories.put(pkeyObj, isDisabled);
			return isDisabled;
		}
	}

	public static void removeCvsRepository(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		// Security checks
		int lsa=getLinuxServerAccountForCvsRepository(conn, pkey);
		LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "removeCvsRepository", lsa);

		removeCvsRepository(conn, invalidateList, pkey);
	}

	public static void removeCvsRepository(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		// Grab values for later use
		int lsa=getLinuxServerAccountForCvsRepository(conn, pkey);
		int aoServer=LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, lsa);

		// Update the database
		conn.executeUpdate("delete from cvs_repositories where pkey=?", pkey);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.CVS_REPOSITORIES,
			LinuxAccountHandler.getBusinessForLinuxServerAccount(conn, lsa),
			aoServer,
			false
		);
	}

	public static void setMode(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		long mode
	) throws IOException, SQLException {
		// Security checks
		int lsa=getLinuxServerAccountForCvsRepository(conn, pkey);
		LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "setMode", lsa);

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
		conn.executeUpdate(
			"update cvs_repositories set mode=? where pkey=?",
			mode,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.CVS_REPOSITORIES,
			LinuxAccountHandler.getBusinessForLinuxServerAccount(conn, lsa),
			LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, lsa),
			false
		);
	}

	/**
	 * Make no instances.
	 */
	private CvsHandler() {
	}
}
