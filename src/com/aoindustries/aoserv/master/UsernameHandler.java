/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.linux.LinuxAccount;
import com.aoindustries.aoserv.client.schema.SchemaTable;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.MySQLUserId;
import com.aoindustries.aoserv.client.validator.PostgresUserId;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.util.IntList;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The <code>UsernameHandler</code> handles all the accesses to the <code>account."Username"</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class UsernameHandler {

	private UsernameHandler() {
	}

	private final static Map<UserId,Boolean> disabledUsernames=new HashMap<>();
	private final static Map<UserId,AccountingCode> usernameBusinesses=new HashMap<>();

	public static boolean canAccessUsername(DatabaseConnection conn, RequestSource source, UserId username) throws IOException, SQLException {
		return PackageHandler.canAccessPackage(conn, source, getPackageForUsername(conn, username));
	}

	public static void checkAccessUsername(DatabaseConnection conn, RequestSource source, String action, UserId username) throws IOException, SQLException {
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
		AccountingCode packageName, 
		UserId username,
		boolean avoidSecurityChecks
	) throws IOException, SQLException {
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add Username for user '"+LinuxAccount.MAIL+'\'');

		if(!avoidSecurityChecks) {
			PackageHandler.checkAccessPackage(conn, source, "addUsername", packageName);
			if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add Username '"+username+"', Package disabled: "+packageName);

			// Make sure people don't create @hostname.com account.Username for domains they cannot control
			String usernameStr = username.toString();
			int atPos=usernameStr.lastIndexOf('@');
			if(atPos!=-1) {
				String hostname=usernameStr.substring(atPos+1);
				if(hostname.length()>0) MasterServer.checkAccessHostname(conn, source, "addUsername", hostname);
			}
		}

		conn.executeUpdate(
			"insert into account.\"Username\" values(?,?,null)",
			username,
			packageName
		);

		// Notify all clients of the update
		AccountingCode accounting = PackageHandler.getBusinessForPackage(conn, packageName);
		invalidateList.addTable(conn, SchemaTable.TableID.USERNAMES, accounting, InvalidateList.allServers, false);
		//invalidateList.addTable(conn, SchemaTable.TableID.PACKAGES, accounting, null);
	}

	public static void disableUsername(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		UserId username
	) throws IOException, SQLException {
		if(isUsernameDisabled(conn, username)) throw new SQLException("Username is already disabled: "+username);
		BusinessHandler.checkAccessDisableLog(conn, source, "disableUsername", disableLog, false);
		checkAccessUsername(conn, source, "disableUsername", username);
		if(
			LinuxAccountHandler.isLinuxAccount(conn, username)
			&& !LinuxAccountHandler.isLinuxAccountDisabled(conn, username)
		) throw new SQLException("Cannot disable Username '"+username+"': LinuxAccount not disabled: "+username);
		try {
			if(
				MySQLHandler.isMySQLUser(conn, username)
				&& !MySQLHandler.isMySQLUserDisabled(conn, MySQLUserId.valueOf(username.toString()))
			) throw new SQLException("Cannot disable Username '"+username+"': MySQLUser not disabled: "+username);
			if(
				PostgresHandler.isPostgresUser(conn, username)
				&& !PostgresHandler.isPostgresUserDisabled(conn, PostgresUserId.valueOf(username.toString()))
			) throw new SQLException("Cannot disable Username '"+username+"': PostgresUser not disabled: "+username);
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}

		conn.executeUpdate(
			"update account.\"Username\" set disable_log=? where username=?",
			disableLog,
			username
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.USERNAMES,
			getBusinessForUsername(conn, username),
			getServersForUsername(conn, username),
			false
		);
	}

	public static void enableUsername(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		UserId username
	) throws IOException, SQLException {
		int disableLog=getDisableLogForUsername(conn, username);
		if(disableLog==-1) throw new SQLException("Username is already enabled: "+username);
		BusinessHandler.checkAccessDisableLog(conn, source, "enableUsername", disableLog, true);
		checkAccessUsername(conn, source, "enableUsername", username);
		AccountingCode pk=getPackageForUsername(conn, username);
		if(PackageHandler.isPackageDisabled(conn, pk)) throw new SQLException("Unable to enable Username '"+username+"', Package not enabled: "+pk);

		conn.executeUpdate(
			"update account.\"Username\" set disable_log=null where username=?",
			username
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.USERNAMES,
			getBusinessForUsername(conn, username),
			getServersForUsername(conn, username),
			false
		);
	}

	public static int getDisableLogForUsername(DatabaseConnection conn, UserId username) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from account.\"Username\" where username=?", username);
	}

	public static void invalidateTable(SchemaTable.TableID tableID) {
		if(tableID==SchemaTable.TableID.USERNAMES) {
			synchronized(disabledUsernames) {
				disabledUsernames.clear();
			}
			synchronized(usernameBusinesses) {
				usernameBusinesses.clear();
			}
		}
	}

	public static boolean isUsernameAvailable(DatabaseConnection conn, UserId username) throws IOException, SQLException {
		return conn.executeBooleanQuery("select (select username from account.\"Username\" where username=?) is null", username);
	}

	public static boolean isUsernameDisabled(DatabaseConnection conn, UserId username) throws IOException, SQLException {
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
		UserId username
	) throws IOException, SQLException {
		if(username.equals(source.getUsername())) throw new SQLException("Not allowed to remove self: "+username);
		checkAccessUsername(conn, source, "removeUsername", username);

		removeUsername(conn, invalidateList, username);
	}

	public static void removeUsername(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		UserId username
	) throws IOException, SQLException {
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to remove Username named '"+LinuxAccount.MAIL+'\'');

		AccountingCode accounting = getBusinessForUsername(conn, username);

		conn.executeUpdate("delete from account.\"Username\" where username=?", username);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.USERNAMES, accounting, InvalidateList.allServers, false);
	}

	public static AccountingCode getBusinessForUsername(DatabaseConnection conn, UserId username) throws IOException, SQLException {
		synchronized(usernameBusinesses) {
			AccountingCode O=usernameBusinesses.get(username);
			if(O!=null) return O;
			AccountingCode accounting = conn.executeObjectQuery(
				ObjectFactories.accountingCodeFactory,
				"select pk.accounting from account.\"Username\" un, billing.\"Package\" pk where un.username=? and un.package=pk.name",
				username
			);
			usernameBusinesses.put(username, accounting);
			return accounting;
		}
	}

	public static AccountingCode getPackageForUsername(DatabaseConnection conn, UserId username) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select package from account.\"Username\" where username=?",
			username
		);
	}

	public static IntList getServersForUsername(DatabaseConnection conn, UserId username) throws IOException, SQLException {
		return conn.executeIntListQuery(
			"select\n"
			+ "  bs.server\n"
			+ "from\n"
			+ "  account.\"Username\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ "  account.\"AccountHost\" bs\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and pk.accounting=bs.accounting",
			username
		);
	}

	public static List<UserId> getUsernamesForPackage(DatabaseConnection conn, AccountingCode name) throws IOException, SQLException {
		return conn.executeObjectListQuery(
			ObjectFactories.userIdFactory,
			"select username from account.\"Username\" where package=?",
			name
		);
	}

	public static boolean canUsernameAccessServer(DatabaseConnection conn, UserId username, int server) throws IOException, SQLException {
		return conn.executeBooleanQuery(
			"select\n"
			+ "  (\n"
			+ "    select\n"
			+ "      un.username\n"
			+ "    from\n"
			+ "      account.\"Username\" un,\n"
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

	public static void checkUsernameAccessServer(DatabaseConnection conn, RequestSource source, String action, UserId username, int server) throws IOException, SQLException {
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
