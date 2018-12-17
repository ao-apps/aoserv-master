/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.account.User;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.util.IntList;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The <code>UsernameHandler</code> handles all the accesses to the <code>account.User</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class UsernameHandler {

	private UsernameHandler() {
	}

	private final static Map<User.Name,Boolean> disabledUsernames=new HashMap<>();
	private final static Map<User.Name,Account.Name> usernameBusinesses=new HashMap<>();

	public static boolean canAccessUsername(DatabaseConnection conn, RequestSource source, User.Name username) throws IOException, SQLException {
		return PackageHandler.canAccessPackage(conn, source, getPackageForUsername(conn, username));
	}

	public static void checkAccessUsername(DatabaseConnection conn, RequestSource source, String action, User.Name username) throws IOException, SQLException {
		if(!canAccessUsername(conn, source, username)) {
			String message=
				"business_administrator.username="
				+source.getUsername()
				+" is not allowed to access username: action='"
				+action
				+"', username="
				+username
			;
			throw new SQLException(message);
		}
	}

	public static void addUsername(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		Account.Name packageName, 
		User.Name username,
		boolean avoidSecurityChecks
	) throws IOException, SQLException {
		if(username.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) throw new SQLException("Not allowed to add Username for user '"+com.aoindustries.aoserv.client.linux.User.MAIL+'\'');

		if(!avoidSecurityChecks) {
			PackageHandler.checkAccessPackage(conn, source, "addUsername", packageName);
			if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add Username '"+username+"', Package disabled: "+packageName);

			// Make sure people don't create @hostname.com account.User for domains they cannot control
			String usernameStr = username.toString();
			int atPos=usernameStr.lastIndexOf('@');
			if(atPos!=-1) {
				String hostname=usernameStr.substring(atPos+1);
				if(hostname.length()>0) MasterServer.checkAccessHostname(conn, source, "addUsername", hostname);
			}
		}

		conn.executeUpdate(
			"insert into account.\"User\" values(?,?,null)",
			username,
			packageName
		);

		// Notify all clients of the update
		Account.Name accounting = PackageHandler.getBusinessForPackage(conn, packageName);
		invalidateList.addTable(conn, Table.TableID.USERNAMES, accounting, InvalidateList.allServers, false);
		//invalidateList.addTable(conn, Table.TableID.PACKAGES, accounting, null);
	}

	public static void disableUsername(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		User.Name username
	) throws IOException, SQLException {
		if(isUsernameDisabled(conn, username)) throw new SQLException("Username is already disabled: "+username);
		BusinessHandler.checkAccessDisableLog(conn, source, "disableUsername", disableLog, false);
		checkAccessUsername(conn, source, "disableUsername", username);
		String un = username.toString();
		if(com.aoindustries.aoserv.client.linux.User.Name.validate(un).isValid()) {
			com.aoindustries.aoserv.client.linux.User.Name linuxUsername;
			try {
				linuxUsername = com.aoindustries.aoserv.client.linux.User.Name.valueOf(un);
			} catch(ValidationException e) {
				throw new AssertionError("Already validated", e);
			}
			if(
				LinuxAccountHandler.isLinuxAccount(conn, linuxUsername)
				&& !LinuxAccountHandler.isLinuxAccountDisabled(conn, linuxUsername)
			) throw new SQLException("Cannot disable Username '"+username+"': Linux user not disabled: "+linuxUsername);
		}
		if(com.aoindustries.aoserv.client.mysql.User.Name.validate(un).isValid()) {
			com.aoindustries.aoserv.client.mysql.User.Name mysqlUsername;
			try {
				mysqlUsername = com.aoindustries.aoserv.client.mysql.User.Name.valueOf(un);
			} catch(ValidationException e) {
				throw new AssertionError("Already validated", e);
			}
			if(
				MySQLHandler.isMySQLUser(conn, mysqlUsername)
				&& !MySQLHandler.isMySQLUserDisabled(conn, mysqlUsername)
			) throw new SQLException("Cannot disable Username '"+username+"': MySQL user not disabled: "+mysqlUsername);
		}
		if(com.aoindustries.aoserv.client.postgresql.User.Name.validate(un).isValid()) {
			com.aoindustries.aoserv.client.postgresql.User.Name postgresqlUsername;
			try {
				postgresqlUsername = com.aoindustries.aoserv.client.postgresql.User.Name.valueOf(un);
			} catch(ValidationException e) {
				throw new AssertionError("Already validated", e);
			}
			if(
				PostgresHandler.isPostgresUser(conn, postgresqlUsername)
				&& !PostgresHandler.isPostgresUserDisabled(conn, postgresqlUsername)
			) throw new SQLException("Cannot disable Username '"+username+"': PostgreSQL user not disabled: "+postgresqlUsername);
		}
		conn.executeUpdate(
			"update account.\"User\" set disable_log=? where username=?",
			disableLog,
			username
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.USERNAMES,
			getBusinessForUsername(conn, username),
			getServersForUsername(conn, username),
			false
		);
	}

	public static void enableUsername(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		User.Name username
	) throws IOException, SQLException {
		int disableLog=getDisableLogForUsername(conn, username);
		if(disableLog==-1) throw new SQLException("Username is already enabled: "+username);
		BusinessHandler.checkAccessDisableLog(conn, source, "enableUsername", disableLog, true);
		checkAccessUsername(conn, source, "enableUsername", username);
		Account.Name pk=getPackageForUsername(conn, username);
		if(PackageHandler.isPackageDisabled(conn, pk)) throw new SQLException("Unable to enable Username '"+username+"', Package not enabled: "+pk);

		conn.executeUpdate(
			"update account.\"User\" set disable_log=null where username=?",
			username
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.USERNAMES,
			getBusinessForUsername(conn, username),
			getServersForUsername(conn, username),
			false
		);
	}

	public static int getDisableLogForUsername(DatabaseConnection conn, User.Name username) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from account.\"User\" where username=?", username);
	}

	public static void invalidateTable(Table.TableID tableID) {
		if(tableID==Table.TableID.USERNAMES) {
			synchronized(disabledUsernames) {
				disabledUsernames.clear();
			}
			synchronized(usernameBusinesses) {
				usernameBusinesses.clear();
			}
		}
	}

	public static boolean isUsernameAvailable(DatabaseConnection conn, User.Name username) throws IOException, SQLException {
		return conn.executeBooleanQuery("select (select username from account.\"User\" where username=?) is null", username);
	}

	public static boolean isUsernameDisabled(DatabaseConnection conn, User.Name username) throws IOException, SQLException {
		synchronized(disabledUsernames) {
			Boolean O=disabledUsernames.get(username);
			if(O!=null) return O;
			boolean isDisabled=getDisableLogForUsername(conn, username)!=-1;
			disabledUsernames.put(username, isDisabled);
			return isDisabled;
		}
	}

	public static void removeUsername(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		User.Name username
	) throws IOException, SQLException {
		if(username.equals(source.getUsername())) throw new SQLException("Not allowed to remove self: "+username);
		checkAccessUsername(conn, source, "removeUsername", username);

		removeUsername(conn, invalidateList, username);
	}

	public static void removeUsername(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		User.Name username
	) throws IOException, SQLException {
		if(username.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) throw new SQLException("Not allowed to remove Username named '"+com.aoindustries.aoserv.client.linux.User.MAIL+'\'');

		Account.Name accounting = getBusinessForUsername(conn, username);

		conn.executeUpdate("delete from account.\"User\" where username=?", username);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.USERNAMES, accounting, InvalidateList.allServers, false);
	}

	public static Account.Name getBusinessForUsername(DatabaseConnection conn, User.Name username) throws IOException, SQLException {
		synchronized(usernameBusinesses) {
			Account.Name O=usernameBusinesses.get(username);
			if(O!=null) return O;
			Account.Name accounting = conn.executeObjectQuery(ObjectFactories.accountNameFactory,
				"select pk.accounting from account.\"User\" un, billing.\"Package\" pk where un.username=? and un.package=pk.name",
				username
			);
			usernameBusinesses.put(username, accounting);
			return accounting;
		}
	}

	// TODO: Cache this lookup, since it is involved iteratively when querying master processes
	public static Account.Name getPackageForUsername(DatabaseConnection conn, User.Name username) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.accountNameFactory,
			"select package from account.\"User\" where username=?",
			username
		);
	}

	public static IntList getServersForUsername(DatabaseConnection conn, User.Name username) throws IOException, SQLException {
		return conn.executeIntListQuery(
			"select\n"
			+ "  bs.server\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ "  account.\"AccountHost\" bs\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and pk.accounting=bs.accounting",
			username
		);
	}

	public static List<User.Name> getUsernamesForPackage(DatabaseConnection conn, Account.Name name) throws IOException, SQLException {
		return conn.executeObjectListQuery(
			ObjectFactories.userNameFactory,
			"select username from account.\"User\" where package=?",
			name
		);
	}

	public static boolean canUsernameAccessServer(DatabaseConnection conn, User.Name username, int server) throws IOException, SQLException {
		return conn.executeBooleanQuery(
			"select\n"
			+ "  (\n"
			+ "    select\n"
			+ "      un.username\n"
			+ "    from\n"
			+ "      account.\"User\" un,\n"
			+ "      billing.\"Package\" pk,\n"
			+ "      account.\"AccountHost\" bs\n"
			+ "    where\n"
			+ "      un.username=?\n"
			+ "      and un.package=pk.name\n"
			+ "      and pk.accounting=bs.accounting\n"
			+ "      and bs.server=?\n"
			+ "    limit 1\n"
			+ "  )\n"
			+ "  is not null\n",
			username,
			server
		);
	}

	public static void checkUsernameAccessServer(DatabaseConnection conn, RequestSource source, String action, User.Name username, int server) throws IOException, SQLException {
		if(!canUsernameAccessServer(conn, username, server)) {
			String message=
			"username="
			+username
			+" is not allowed to access server.id="
			+server
			+": action='"
			+action
			+"'"
			;
			throw new SQLException(message);
		}
	}
}
