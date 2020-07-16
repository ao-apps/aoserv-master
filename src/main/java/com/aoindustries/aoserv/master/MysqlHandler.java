/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2016, 2017, 2018, 2019, 2020  AO Industries, Inc.
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

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.backup.MysqlReplication;
import com.aoindustries.aoserv.client.linux.PosixPath;
import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.mysql.Database;
import com.aoindustries.aoserv.client.mysql.Server;
import com.aoindustries.aoserv.client.mysql.Table_Name;
import com.aoindustries.aoserv.client.mysql.UserServer;
import com.aoindustries.aoserv.client.password.PasswordChecker;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import com.aoindustries.collections.IntList;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.stream.StreamableOutput;
import com.aoindustries.net.Port;
import com.aoindustries.util.Tuple2;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The <code>MySQLHandler</code> handles all the accesses to the MySQL tables.
 *
 * @author  AO Industries, Inc.
 */
final public class MysqlHandler {

	private MysqlHandler() {
	}

	private final static Map<Integer,Boolean> disabledUserServers = new HashMap<>();
	private final static Map<com.aoindustries.aoserv.client.mysql.User.Name,Boolean> disabledUsers = new HashMap<>();

	public static void checkAccessDatabase(DatabaseConnection conn, RequestSource source, String action, int database) throws IOException, SQLException {
		User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getCurrentAdministrator()).length!=0) {
				int mysqlServer = getServerForDatabase(conn, database);
				int linuxServer = getLinuxServerForServer(conn, mysqlServer);
				NetHostHandler.checkAccessHost(conn, source, action, linuxServer);
			}
		} else {
			PackageHandler.checkAccessPackage(conn, source, action, getPackageForDatabase(conn, database));
		}
	}

	public static void checkAccessDatabaseUser(DatabaseConnection conn, RequestSource source, String action, int databaseUser) throws IOException, SQLException {
		checkAccessDatabase(conn, source, action, getDatabaseForDatabaseUser(conn, databaseUser));
		checkAccessUserServer(conn, source, action, getUserServerForDatabaseUser(conn, databaseUser));
	}

	public static void checkAccessUserServer(DatabaseConnection conn, RequestSource source, String action, int userServer) throws IOException, SQLException {
		User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getCurrentAdministrator()).length!=0) {
				int mysqlServer = getServerForUserServer(conn, userServer);
				int linuxServer = getLinuxServerForServer(conn, mysqlServer);
				NetHostHandler.checkAccessHost(conn, source, action, linuxServer);
			}
		} else {
			checkAccessUser(conn, source, action, getUserForUserServer(conn, userServer));
		}
	}

	public static void checkAccessServer(DatabaseConnection conn, RequestSource source, String action, int mysqlServer) throws IOException, SQLException {
		User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getCurrentAdministrator()).length!=0) {
				// Protect by server
				int linuxServer = getLinuxServerForServer(conn, mysqlServer);
				NetHostHandler.checkAccessHost(conn, source, action, linuxServer);
			}
		} else {
			// Protect by package
			Account.Name packageName = getPackageForServer(conn, mysqlServer);
			PackageHandler.checkAccessPackage(conn, source, action, packageName);
		}
	}

	public static void checkAccessUser(DatabaseConnection conn, RequestSource source, String action, com.aoindustries.aoserv.client.mysql.User.Name user) throws IOException, SQLException {
		User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getCurrentAdministrator()).length!=0) {
				IntList msus = getUserServersForUser(conn, user);
				boolean found = false;
				for(int msu : msus) {
					int mysqlServer = getServerForUserServer(conn, msu);
					int linuxServer = getLinuxServerForServer(conn, mysqlServer);
					if(NetHostHandler.canAccessHost(conn, source, linuxServer)) {
						found=true;
						break;
					}
				}
				if(!found) {
					String message=
						"currentAdministrator="
						+source.getCurrentAdministrator()
						+" is not allowed to access mysql_user: action='"
						+action
						+", user="
						+user
					;
					throw new SQLException(message);
				}
			}
		} else {
			AccountUserHandler.checkAccessUser(conn, source, action, user);
		}
	}

	/**
	 * Adds a MySQL database to the system.
	 */
	public static int addDatabase(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Database.Name name,
		int mysqlServer,
		Account.Name packageName
	) throws IOException, SQLException {
		if(Database.isSpecial(name)) {
			throw new SQLException("Refusing to add special MySQL database: " + name);
		}

		int linuxServer = getLinuxServerForServer(conn, mysqlServer);

		PackageHandler.checkPackageAccessHost(conn, source, "addDatabase", packageName, linuxServer);
		if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add Database '"+name+"', Package disabled: "+packageName);

		// Must be allowed to access this server and package
		NetHostHandler.checkAccessHost(conn, source, "addDatabase", linuxServer);
		PackageHandler.checkAccessPackage(conn, source, "addDatabase", packageName);

		// Find the accouting code
		Account.Name account = PackageHandler.getAccountForPackage(conn, packageName);
		// This sub-account must have access to the server
		AccountHandler.checkAccountAccessHost(conn, source, "addDatabase", account , linuxServer);

		// Add the entry to the database
		int database = conn.executeIntUpdate(
			"INSERT INTO\n"
			+ "  mysql.\"Database\"\n"
			+ "VALUES (\n"
			+ "  default,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?\n"
			+ ") RETURNING id",
			name,
			mysqlServer,
			packageName
		);

		// Notify all clients of the update, the server will detect this change and automatically add the database
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_DATABASES,
			account ,
			linuxServer,
			false
		);
		return database;
	}

	/**
	 * Grants a UserServer access to a MySQLMasterDatabase.getDatabase().
	 */
	public static int addDatabaseUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int database,
		int userServer,
		boolean canSelect,
		boolean canInsert,
		boolean canUpdate,
		boolean canDelete,
		boolean canCreate,
		boolean canDrop,
		boolean canReference,
		boolean canIndex,
		boolean canAlter,
		boolean canCreateTempTable,
		boolean canLockTables,
		boolean canCreateView,
		boolean canShowView,
		boolean canCreateRoutine,
		boolean canAlterRoutine,
		boolean canExecute,
		boolean canEvent,
		boolean canTrigger
	) throws IOException, SQLException {
		// Must be allowed to access this database and user
		checkAccessDatabase(conn, source, "addDatabaseUser", database);
		checkAccessUserServer(conn, source, "addDatabaseUser", userServer);
		if(isUserServerDisabled(conn, userServer)) throw new SQLException("Unable to add DatabaseUser, UserServer disabled: " + userServer);

		com.aoindustries.aoserv.client.mysql.User.Name user = getUserForUserServer(conn, userServer);
		if(com.aoindustries.aoserv.client.mysql.User.isSpecial(user)) {
			throw new SQLException("Refusing to grant access to a special MySQL user: " + user);
		}
		Database.Name name = getNameForDatabase(conn, database);
		if(Database.isSpecial(name)) {
			throw new SQLException("Refusing to grant access to a special MySQL database: " + name);
		}

		// Must also have matching servers
		int database_server = getServerForDatabase(conn, database);
		int userServer_server = getServerForUserServer(conn, userServer);
		// TODO: Enforce this with PostgreSQL trigger
		if(database_server != userServer_server) throw new SQLException("Mismatched mysql.Server for mysql.Database and mysql.UserServer");

		// Add the entry to the database
		int databaseUser = conn.executeIntUpdate(
			"INSERT INTO mysql.\"DatabaseUser\" VALUES (default,?,?,?,?,?,?,?,?,false,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id",
			database,
			userServer,
			canSelect,
			canInsert,
			canUpdate,
			canDelete,
			canCreate,
			canDrop,
			canReference,
			canIndex,
			canAlter,
			canCreateTempTable,
			canLockTables,
			canCreateView,
			canShowView,
			canCreateRoutine,
			canAlterRoutine,
			canExecute,
			canEvent,
			canTrigger
		);

		// Notify all clients of the update, the server will detect this change and automatically update MySQL
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_DB_USERS,
			getAccountForUserServer(conn, userServer),
			getLinuxServerForServer(conn, database_server),
			false
		);
		return databaseUser;
	}

	/**
	 * Adds a MySQL server user.
	 */
	public static int addUserServer(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.mysql.User.Name user,
		int mysqlServer,
		String host
	) throws IOException, SQLException {
		if(com.aoindustries.aoserv.client.mysql.User.isSpecial(user)) {
			throw new SQLException("Refusing to add special MySQL user: " + user);
		}
		if(user.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) throw new SQLException("Not allowed to add UserServer for user '"+com.aoindustries.aoserv.client.linux.User.MAIL+'\'');

		checkAccessUser(conn, source, "addUserServer", user);
		if(isUserDisabled(conn, user)) throw new SQLException("Unable to add UserServer, User disabled: "+user);
		int linuxServer = getLinuxServerForServer(conn, mysqlServer);
		NetHostHandler.checkAccessHost(conn, source, "addUserServer", linuxServer);
		// This sub-account must have access to the server
		AccountUserHandler.checkUserAccessHost(conn, source, "addUserServer", user, linuxServer);

		int userServer = conn.executeIntUpdate(
			"INSERT INTO mysql.\"UserServer\" VALUES(default,?,?,?,null,null,?,?,?,?) RETURNING id",
			user,
			mysqlServer,
			host,
			UserServer.DEFAULT_MAX_QUESTIONS,
			UserServer.DEFAULT_MAX_UPDATES,
			UserServer.DEFAULT_MAX_CONNECTIONS,
			UserServer.DEFAULT_MAX_USER_CONNECTIONS
		);

		// Notify all clients of the update
		Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_SERVER_USERS,
			account,
			linuxServer,
			true
		);
		return userServer;
	}

	/**
	 * Adds a MySQL user.
	 */
	public static void addUser(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.mysql.User.Name user
	) throws IOException, SQLException {
		if(com.aoindustries.aoserv.client.mysql.User.isSpecial(user)) {
			throw new SQLException("Refusing to add special MySQL user: " + user);
		}
		if(user.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) throw new SQLException("Not allowed to add User for user '"+com.aoindustries.aoserv.client.linux.User.MAIL+'\'');

		AccountUserHandler.checkAccessUser(conn, source, "addUser", user);
		if(AccountUserHandler.isUserDisabled(conn, user)) throw new SQLException("Unable to add User, Username disabled: "+user);

		conn.executeUpdate(
			"insert into mysql.\"User\"(username) values(?)",
			user
		);

		// Notify all clients of the update
		invalidateList.addTable(conn,
			Table.TableID.MYSQL_USERS,
			AccountUserHandler.getAccountForUser(conn, user),
			InvalidateList.allHosts,
			true
		);
	}

	public static void disableUserServer(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		int userServer
	) throws IOException, SQLException {
		AccountHandler.checkAccessDisableLog(conn, source, "disableUserServer", disableLog, false);
		checkAccessUserServer(conn, source, "disableUserServer", userServer);

		com.aoindustries.aoserv.client.mysql.User.Name mu = getUserForUserServer(conn, userServer);
		if(com.aoindustries.aoserv.client.mysql.User.isSpecial(mu)) {
			throw new SQLException("Refusing to disable special MySQL user: " + mu);
		}
		if(isUserServerDisabled(conn, userServer)) throw new SQLException("UserServer is already disabled: "+userServer);

		conn.executeUpdate(
			"update mysql.\"UserServer\" set disable_log=? where id=?",
			disableLog,
			userServer
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_SERVER_USERS,
			getAccountForUserServer(conn, userServer),
			getLinuxServerForServer(conn, getServerForUserServer(conn, userServer)),
			false
		);
	}

	public static void disableUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		com.aoindustries.aoserv.client.mysql.User.Name user
	) throws IOException, SQLException {
		if(com.aoindustries.aoserv.client.mysql.User.isSpecial(user)) {
			throw new SQLException("Refusing to disable special MySQL user: " + user);
		}

		AccountHandler.checkAccessDisableLog(conn, source, "disableUser", disableLog, false);
		checkAccessUser(conn, source, "disableUser", user);

		if(isUserDisabled(conn, user)) throw new SQLException("User is already disabled: "+user);

		IntList userServers = getUserServersForUser(conn, user);
		for(int c=0;c<userServers.size();c++) {
			int serverUser = userServers.getInt(c);
			if(!isUserServerDisabled(conn, serverUser)) {
				throw new SQLException("Cannot disable User '"+user+"': UserServer not disabled: "+serverUser);
			}
		}

		conn.executeUpdate(
			"update mysql.\"User\" set disable_log=? where username=?",
			disableLog,
			user
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_USERS,
			AccountUserHandler.getAccountForUser(conn, user),
			AccountUserHandler.getHostsForUser(conn, user),
			false
		);
	}

	/**
	 * Dumps a MySQL database
	 */
	public static void dumpDatabase(
		DatabaseConnection conn,
		RequestSource source,
		StreamableOutput out,
		int database,
		boolean gzip
	) throws IOException, SQLException {
		checkAccessDatabase(conn, source, "dumpDatabase", database);

		int mysqlServer = getServerForDatabase(conn, database);
		int linuxServer = getLinuxServerForServer(conn, mysqlServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.releaseConnection();
		daemonConnector.dumpMySQLDatabase(
			database,
			gzip,
			(long dumpSize) -> {
				if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_0) >= 0) {
					out.writeLong(dumpSize);
				}
			},
			out
		);
	}

	public static void enableUserServer(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int userServer
	) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "enableUserServer", userServer);
		int disableLog = getDisableLogForUserServer(conn, userServer);
		if(disableLog == -1) throw new SQLException("UserServer is already enabled: "+userServer);
		AccountHandler.checkAccessDisableLog(conn, source, "enableUserServer", disableLog, true);

		com.aoindustries.aoserv.client.mysql.User.Name user = getUserForUserServer(conn, userServer);
		if(com.aoindustries.aoserv.client.mysql.User.isSpecial(user)) {
			throw new SQLException("Refusing to enable special MySQL user: " + user);
		}
		if(isUserDisabled(conn, user)) throw new SQLException("Unable to enable UserServer #"+userServer+", User not enabled: "+user);

		conn.executeUpdate(
			"update mysql.\"UserServer\" set disable_log=null where id=?",
			userServer
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_SERVER_USERS,
			AccountUserHandler.getAccountForUser(conn, user),
			getLinuxServerForServer(conn, getServerForUserServer(conn, userServer)),
			false
		);
	}

	public static void enableUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.mysql.User.Name user
	) throws IOException, SQLException {
		if(com.aoindustries.aoserv.client.mysql.User.isSpecial(user)) {
			throw new SQLException("Refusing to enable special MySQL user: " + user);
		}

		AccountUserHandler.checkAccessUser(conn, source, "enableUser", user);
		int disableLog = getDisableLogForUser(conn, user);
		if(disableLog == -1) throw new SQLException("User is already enabled: "+user);
		AccountHandler.checkAccessDisableLog(conn, source, "enableUser", disableLog, true);

		if(AccountUserHandler.isUserDisabled(conn, user)) throw new SQLException("Unable to enable User '"+user+"', Username not enabled: "+user);

		conn.executeUpdate(
			"update mysql.\"User\" set disable_log=null where username=?",
			user
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_USERS,
			AccountUserHandler.getAccountForUser(conn, user),
			AccountUserHandler.getHostsForUser(conn, user),
			false
		);
	}

	/**
	 * Generates a unique MySQL database name.
	 */
	public static Database.Name generateDatabaseName(
		DatabaseConnection conn,
		String template_base,
		String template_added
	) throws IOException, SQLException {
		// Load the entire list of mysql database names
		Set<Database.Name> names = conn.executeObjectCollectionQuery(new HashSet<>(),
			ObjectFactories.mysqlDatabaseNameFactory,
			"select name from mysql.\"Database\" group by name"
		);
		// Find one that is not used
		for(int c=0;c<Integer.MAX_VALUE;c++) {
			Database.Name name;
			try {
				name = Database.Name.valueOf((c==0) ? template_base : (template_base+template_added+c));
			} catch(ValidationException e) {
				throw new SQLException(e.getLocalizedMessage(), e);
			}
			if(!names.contains(name)) return name;
		}
		// If could not find one, report and error
		throw new SQLException("Unable to find available MySQL database name for template_base="+template_base+" and template_added="+template_added);
	}

	public static int getDisableLogForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from mysql.\"UserServer\" where id=?", userServer);
	}

	public static int getDisableLogForUser(DatabaseConnection conn, com.aoindustries.aoserv.client.mysql.User.Name user) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from mysql.\"User\" where username=?", user);
	}

	public static com.aoindustries.aoserv.client.mysql.User.Name getUserForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.mysqlUserNameFactory,
			"select username from mysql.\"UserServer\" where id=?",
			userServer
		);
	}

	public static Database.Name getNameForDatabase(DatabaseConnection conn, int database) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.mysqlDatabaseNameFactory,
			"select name from mysql.\"Database\" where id=?",
			database
		);
	}

	public static void invalidateTable(Table.TableID tableID) {
		switch(tableID) {
			case MYSQL_SERVER_USERS :
				synchronized(MysqlHandler.class) {
					disabledUserServers.clear();
				}
				break;
			case MYSQL_USERS :
				synchronized(MysqlHandler.class) {
					disabledUsers.clear();
				}
				break;
		}
	}

	public static boolean isUserServerDisabled(DatabaseConnection conn, int userServer) throws IOException, SQLException {
		synchronized(MysqlHandler.class) {
			Integer I=userServer;
			Boolean O=disabledUserServers.get(I);
			if(O!=null) return O;
			boolean isDisabled=getDisableLogForUserServer(conn, userServer)!=-1;
			disabledUserServers.put(I, isDisabled);
			return isDisabled;
		}
	}

	public static boolean isUser(DatabaseConnection conn, com.aoindustries.aoserv.client.mysql.User.Name user) throws IOException, SQLException {
		return conn.executeBooleanQuery(
			"select\n"
			+ "  (\n"
			+ "    select\n"
			+ "      username\n"
			+ "    from\n"
			+ "      mysql.\"User\"\n"
			+ "    where\n"
			+ "      username=?\n"
			+ "    limit 1\n"
			+ "  ) is not null",
			user
		);
	}

	public static boolean isUserDisabled(DatabaseConnection conn, com.aoindustries.aoserv.client.mysql.User.Name user) throws IOException, SQLException {
		synchronized(MysqlHandler.class) {
			Boolean O=disabledUsers.get(user);
			if(O!=null) return O;
			boolean isDisabled=getDisableLogForUser(conn, user)!=-1;
			disabledUsers.put(user, isDisabled);
			return isDisabled;
		}
	}

	/**
	 * Determines if a MySQL database name is available.
	 */
	public static boolean isDatabaseNameAvailable(
		DatabaseConnection conn,
		RequestSource source,
		Database.Name name,
		int mysqlServer
	) throws IOException, SQLException {
		int linuxServer = getLinuxServerForServer(conn, mysqlServer);
		NetHostHandler.checkAccessHost(conn, source, "isDatabaseNameAvailable", linuxServer);
		return conn.executeBooleanQuery("select (select id from mysql.\"Database\" where name=? and mysql_server=?) is null", name, mysqlServer);
	}

	public static boolean isUserServerPasswordSet(
		DatabaseConnection conn,
		RequestSource source,
		int userServer
	) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "isUserServerPasswordSet", userServer);
		com.aoindustries.aoserv.client.mysql.User.Name user = getUserForUserServer(conn, userServer);
		if(com.aoindustries.aoserv.client.mysql.User.isSpecial(user)) throw new SQLException("Refusing to check if passwords set on special MySQL user: " + user);
		if(isUserServerDisabled(conn, userServer)) throw new SQLException("Unable to determine if the UserServer password is set, account disabled: " + userServer);
		
		int mysqlServer = getServerForUserServer(conn, userServer);
		int linuxServer = getLinuxServerForServer(conn, mysqlServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.releaseConnection();
		String password = daemonConnector.getEncryptedMySQLUserPassword(mysqlServer, user);
		return !com.aoindustries.aoserv.client.mysql.User.NO_PASSWORD_DB_VALUE.equals(password);
	}

	/**
	 * Removes a Database from the system.
	 */
	public static void removeDatabase(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int database
	) throws IOException, SQLException {
		checkAccessDatabase(conn, source, "removeDatabase", database);

		removeDatabase(conn, invalidateList, database);
	}

	/**
	 * Removes a Database from the system.
	 */
	public static void removeDatabase(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int database
	) throws IOException, SQLException {
		Database.Name name = getNameForDatabase(conn, database);
		if(Database.isSpecial(name)) {
			throw new SQLException("Refusing to remove special MySQL database: " + name);
		}

		// Remove the mysql_db_user entries
		List<Account.Name> dbUserAccounts = conn.executeObjectCollectionQuery(
			new ArrayList<>(),
			ObjectFactories.accountNameFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  mysql.\"DatabaseUser\" mdu,\n"
			+ "  mysql.\"UserServer\" msu,\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk\n"
			+ "where\n"
			+ "  mdu.mysql_database=?\n"
			+ "  and mdu.mysql_server_user=msu.id\n"
			+ "  and msu.username=un.username\n"
			+ "  and un.package=pk.name\n"
			+ "group by\n"
			+ "  pk.accounting",
			database
		);
		if(!dbUserAccounts.isEmpty()) conn.executeUpdate("delete from mysql.\"DatabaseUser\" where mysql_database=?", database);

		// Remove the database entry
		Account.Name account = getAccountForDatabase(conn, database);
		int mysqlServer = getServerForDatabase(conn, database);
		int linuxServer = getLinuxServerForServer(conn, mysqlServer);
		conn.executeUpdate("delete from mysql.\"Database\" where id=?", database);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_DATABASES,
			account,
			linuxServer,
			false
		);
		if(!dbUserAccounts.isEmpty()) invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_DB_USERS,
			dbUserAccounts,
			linuxServer,
			false
		);
	}

	/**
	 * Removes a DatabaseUser from the system.
	 */
	public static void removeDatabaseUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int databaseUser
	) throws IOException, SQLException {
		checkAccessDatabaseUser(conn, source, "removeDatabaseUser", databaseUser);

		com.aoindustries.aoserv.client.mysql.User.Name user = getUserForUserServer(
			conn,
			getUserServerForDatabaseUser(conn, databaseUser)
		);
		if(com.aoindustries.aoserv.client.mysql.User.isSpecial(user)) {
			throw new SQLException("Refusing to revoke access from a special MySQL user: " + user);
		}
		Database.Name database_name = getNameForDatabase(
			conn,
			getDatabaseForDatabaseUser(conn, databaseUser)
		);
		if(Database.isSpecial(database_name)) {
			throw new SQLException("Refusing to revoke access to a special MySQL database: " + database_name);
		}

		// Remove the mysql_db_user
		Account.Name account = getAccountForDatabaseUser(conn, databaseUser);
		int mysqlServer = getServerForDatabaseUser(conn, databaseUser);
		int linuxServer = getLinuxServerForServer(conn, mysqlServer);
		conn.executeUpdate("delete from mysql.\"DatabaseUser\" where id=?", databaseUser);

		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_DB_USERS,
			account,
			linuxServer,
			false
		);
	}

	/**
	 * Removes a UserServer from the system.
	 */
	public static void removeUserServer(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int userServer
	) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "removeUserServer", userServer);

		com.aoindustries.aoserv.client.mysql.User.Name mu = getUserForUserServer(conn, userServer);
		if(com.aoindustries.aoserv.client.mysql.User.isSpecial(mu)) {
			throw new SQLException("Refusing to remove special MySQL user: " + mu);
		}

		// Remove the mysql_db_user
		boolean dbUsersExist=conn.executeBooleanQuery("select (select id from mysql.\"DatabaseUser\" where mysql_server_user=? limit 1) is not null", userServer);
		if(dbUsersExist) conn.executeUpdate("delete from mysql.\"DatabaseUser\" where mysql_server_user=?", userServer);

		// Remove the mysql_server_user
		Account.Name account = getAccountForUserServer(conn, userServer);
		int mysqlServer = getServerForUserServer(conn, userServer);
		int linuxServer = getLinuxServerForServer(conn, mysqlServer);
		conn.executeUpdate("delete from mysql.\"UserServer\" where id=?", userServer);

		// Notify all clients of the updates
		if(dbUsersExist) invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_DB_USERS,
			account,
			linuxServer,
			false
		);
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_SERVER_USERS,
			account,
			linuxServer,
			true
		);
	}

	/**
	 * Removes a User from the system.
	 */
	public static void removeUser(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.mysql.User.Name user
	) throws IOException, SQLException {
		checkAccessUser(conn, source, "removeUser", user);

		removeUser(conn, invalidateList, user);
	}

	/**
	 * Removes a User from the system.
	 */
	public static void removeUser(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.mysql.User.Name user
	) throws IOException, SQLException {
		if(com.aoindustries.aoserv.client.mysql.User.isSpecial(user)) {
			throw new SQLException("Refusing to remove special MySQL user: " + user);
		}
		Account.Name account = AccountUserHandler.getAccountForUser(conn, user);

		// Remove the mysql_db_user
		IntList mysqlServers = conn.executeIntListQuery(
			"select\n"
			+ "  md.mysql_server\n"
			+ "from\n"
			+ "  mysql.\"UserServer\" msu,\n"
			+ "  mysql.\"DatabaseUser\" mdu,\n"
			+ "  mysql.\"Database\" md\n"
			+ "where\n"
			+ "  msu.username=?\n"
			+ "  and msu.id=mdu.mysql_server_user\n"
			+ "  and mdu.mysql_database=md.id\n"
			+ "group by\n"
			+ "  md.mysql_server",
			user
		);
		if(!mysqlServers.isEmpty()) {
			conn.executeUpdate(
				"delete from\n"
				+ "  mysql.\"DatabaseUser\"\n"
				+ "where\n"
				+ "  id in (\n"
				+ "    select\n"
				+ "      mdu.id\n"
				+ "    from\n"
				+ "      mysql.\"UserServer\" msu,\n"
				+ "      mysql.\"DatabaseUser\" mdu\n"
				+ "    where\n"
				+ "      msu.username=?\n"
				+ "      and msu.id=mdu.mysql_server_user"
				+ "  )",
				user
			);
			for(int mysqlServer : mysqlServers) {
				invalidateList.addTable(
					conn,
					Table.TableID.MYSQL_DB_USERS,
					account,
					getLinuxServerForServer(conn, mysqlServer),
					false
				);
			}
		}

		// Remove the mysql_server_user
		mysqlServers = conn.executeIntListQuery("select mysql_server from mysql.\"UserServer\" where username=?", user);
		if(!mysqlServers.isEmpty()) {
			conn.executeUpdate("delete from mysql.\"UserServer\" where username=?", user);
			for(int mysqlServer : mysqlServers) {
				invalidateList.addTable(
					conn,
					Table.TableID.MYSQL_SERVER_USERS,
					account,
					getLinuxServerForServer(conn, mysqlServer),
					false
				);
			}
		}

		// Remove the mysql_user
		conn.executeUpdate("delete from mysql.\"User\" where username=?", user);
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_USERS,
			account,
			AccountHandler.getHostsForAccount(conn, account),
			false
		);
	}

	/**
	 * Sets a MySQL password.
	 */
	public static void setUserServerPassword(
		DatabaseConnection conn,
		RequestSource source,
		int userServer,
		String password
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "setUserServerPassword", Permission.Name.set_mysql_server_user_password);
		checkAccessUserServer(conn, source, "setUserServerPassword", userServer);
		if(isUserServerDisabled(conn, userServer)) throw new SQLException("Unable to set UserServer password, account disabled: "+userServer);

		com.aoindustries.aoserv.client.mysql.User.Name mu = getUserForUserServer(conn, userServer);
		if(com.aoindustries.aoserv.client.mysql.User.isSpecial(mu)) {
			throw new SQLException("Refusing to set the password for a special MySQL user: " + mu);
		}

		// Perform the password check here, too.
		if(password!=null && password.length()==0) password=com.aoindustries.aoserv.client.mysql.User.NO_PASSWORD;
		if(Objects.equals(password, com.aoindustries.aoserv.client.mysql.User.NO_PASSWORD)) {
			List<PasswordChecker.Result> results = com.aoindustries.aoserv.client.mysql.User.checkPassword(mu, password);
			if(PasswordChecker.hasResults(results)) throw new SQLException("Invalid password: "+PasswordChecker.getResultsString(results).replace('\n', '|'));
		}

		// Contact the daemon for the update
		int mysqlServer = getServerForUserServer(conn, userServer);
		int linuxServer = getLinuxServerForServer(conn, mysqlServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.releaseConnection();
		daemonConnector.setMySQLUserPassword(mysqlServer, mu, password);
	}

	public static void setUserServerPredisablePassword(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int userServer,
		String password
	) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "setUserServerPredisablePassword", userServer);

		com.aoindustries.aoserv.client.mysql.User.Name mu = getUserForUserServer(conn, userServer);
		if(com.aoindustries.aoserv.client.mysql.User.isSpecial(mu)) {
			throw new SQLException("May not disable special MySQL user: " + mu);
		}

		if(password==null) {
			if(isUserServerDisabled(conn, userServer)) throw new SQLException("Unable to clear UserServer predisable password, account disabled: "+userServer);
		} else {
			if(!isUserServerDisabled(conn, userServer)) throw new SQLException("Unable to set UserServer predisable password, account not disabled: "+userServer);
		}

		// Update the database
		conn.executeUpdate(
			"update mysql.\"UserServer\" set predisable_password=? where id=?",
			password,
			userServer
		);

		int mysqlServer = getServerForUserServer(conn, userServer);
		int linuxServer = getLinuxServerForServer(conn, mysqlServer);
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_SERVER_USERS,
			getAccountForUserServer(conn, userServer),
			linuxServer,
			false
		);
	}

	/**
	 * Waits for any pending or processing MySQL database config rebuild to complete.
	 */
	public static void waitForDatabaseRebuild(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer
	) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "waitForDatabaseRebuild", linuxServer);
		NetHostHandler.waitForInvalidates(linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.releaseConnection();
		daemonConnector.waitForMySQLDatabaseRebuild();
	}

	/**
	 * Waits for any pending or processing MySQL database config rebuild to complete.
	 */
	public static void waitForDatabaseUserRebuild(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer
	) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "waitForDatabaseUserRebuild", linuxServer);
		NetHostHandler.waitForInvalidates(linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.releaseConnection();
		daemonConnector.waitForMySQLDBUserRebuild();
	}

	public static void waitForServerRebuild(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer
	) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "waitForServerRebuild", linuxServer);
		NetHostHandler.waitForInvalidates(linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.releaseConnection();
		daemonConnector.waitForMySQLServerRebuild();
	}

	/**
	 * Waits for any pending or processing MySQL database config rebuild to complete.
	 */
	public static void waitForUserRebuild(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer
	) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "waitForUserRebuild", linuxServer);
		NetHostHandler.waitForInvalidates(linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.releaseConnection();
		daemonConnector.waitForMySQLUserRebuild();
	}

	public static Account.Name getAccountForDatabase(DatabaseConnection conn, int database) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountNameFactory,
			"select pk.accounting from mysql.\"Database\" md, billing.\"Package\" pk where md.package=pk.name and md.id=?",
			database
		);
	}

	public static Account.Name getAccountForDatabaseUser(DatabaseConnection conn, int databaseUser) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountNameFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  mysql.\"DatabaseUser\" mdu,\n"
			+ "  mysql.\"UserServer\" msu,\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk\n"
			+ "where\n"
			+ "  mdu.id=?\n"
			+ "  and mdu.mysql_server_user=msu.id\n"
			+ "  and msu.username=un.username\n"
			+ "  and un.package=pk.name",
			databaseUser
		);
	}

	public static Account.Name getAccountForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountNameFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  mysql.\"UserServer\" msu,\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk\n"
			+ "where\n"
			+ "  msu.username=un.username\n"
			+ "  and un.package=pk.name\n"
			+ "  and msu.id=?",
			userServer
		);
	}

	public static int getPackageForDatabase(DatabaseConnection conn, int database) throws IOException, SQLException {
		return conn.executeIntQuery("select pk.id from mysql.\"Database\" md, billing.\"Package\" pk where md.id=? and md.package=pk.name", database);
	}

	public static IntList getUserServersForUser(DatabaseConnection conn, com.aoindustries.aoserv.client.mysql.User.Name user) throws IOException, SQLException {
		return conn.executeIntListQuery("select id from mysql.\"UserServer\" where username=?", user);
	}

	public static int getServerForDatabase(DatabaseConnection conn, int database) throws IOException, SQLException {
		return conn.executeIntQuery("select mysql_server from mysql.\"Database\" where id=?", database);
	}

	public static int getLinuxServerForServer(DatabaseConnection conn, int mysqlServer) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from mysql.\"Server\" where bind=?", mysqlServer);
	}

	public static Account.Name getPackageForServer(DatabaseConnection conn, int mysqlServer) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountNameFactory,
			"select\n"
			+ "  nb.package\n"
			+ "from\n"
			+ "  mysql.\"Server\" ms\n"
			+ "  inner join net.\"Bind\" nb on ms.bind=nb.id\n"
			+ "where\n"
			+ "  ms.bind=?",
			mysqlServer
		);
	}

	public static Tuple2<Server.Name,Port> getNameAndPortForServer(DatabaseConnection conn, int mysqlServer) throws IOException, SQLException {
		return conn.executeObjectQuery(
			(ResultSet result) -> new Tuple2<>(
				ObjectFactories.mysqlServerNameFactory.createObject(result),
				ObjectFactories.portFactory.createObject(result)
			),
			"select\n"
			+ "  ms.\"name\" as \"mysqlServerName\","
			+ "  nb.port,\n"
			+ "  nb.net_protocol\n"
			+ "from\n"
			+ "  mysql.\"Server\" ms\n"
			+ "  inner join net.\"Bind\" nb on ms.bind=nb.id\n"
			+ "where\n"
			+ "  ms.bind=?",
			mysqlServer
		);
	}

	public static int getServerForBackupMysqlReplication(DatabaseConnection conn, int mysqlReplication) throws IOException, SQLException {
		return conn.executeIntQuery("select mysql_server from backup.\"MysqlReplication\" where id=?", mysqlReplication);
	}

	public static int getServerForDatabaseUser(DatabaseConnection conn, int databaseUser) throws IOException, SQLException {
		return conn.executeIntQuery("select msu.mysql_server from mysql.\"DatabaseUser\" mdu, mysql.\"UserServer\" msu where mdu.id=? and mdu.mysql_server_user=msu.id", databaseUser);
	}

	public static int getDatabaseForDatabaseUser(DatabaseConnection conn, int databaseUser) throws IOException, SQLException {
		return conn.executeIntQuery("select mysql_database from mysql.\"DatabaseUser\" where id=?", databaseUser);
	}

	public static int getUserServerForDatabaseUser(DatabaseConnection conn, int databaseUser) throws IOException, SQLException {
		return conn.executeIntQuery("select mysql_server_user from mysql.\"DatabaseUser\" where id=?", databaseUser);
	}

	public static int getServerForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
		return conn.executeIntQuery("select mysql_server from mysql.\"UserServer\" where id=?", userServer);
	}

	public static void restartServer(
		DatabaseConnection conn,
		RequestSource source,
		int mysqlServer
	) throws IOException, SQLException {
		int linuxServer = getLinuxServerForServer(conn, mysqlServer);
		boolean canControl = AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_mysql");
		if(!canControl) throw new SQLException("Not allowed to restart MySQL on "+linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.releaseConnection();
		daemonConnector.restartMySQL(mysqlServer);
	}

	public static void startServer(
		DatabaseConnection conn,
		RequestSource source,
		int mysqlServer
	) throws IOException, SQLException {
		int linuxServer = getLinuxServerForServer(conn, mysqlServer);
		boolean canControl = AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_mysql");
		if(!canControl) throw new SQLException("Not allowed to start MySQL on "+linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.releaseConnection();
		daemonConnector.startMySQL(mysqlServer);
	}

	public static void stopServer(
		DatabaseConnection conn,
		RequestSource source,
		int mysqlServer
	) throws IOException, SQLException {
		int linuxServer = getLinuxServerForServer(conn, mysqlServer);
		boolean canControl = AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_mysql");
		if(!canControl) throw new SQLException("Not allowed to stop MySQL on "+linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.releaseConnection();
		daemonConnector.stopMySQL(mysqlServer);
	}

	public static void getMasterStatus(
		DatabaseConnection conn,
		RequestSource source,
		int mysqlServer,
		StreamableOutput out
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "getMasterStatus", Permission.Name.get_mysql_master_status);
		// Check access
		checkAccessServer(conn, source, "getMasterStatus", mysqlServer);
		int linuxServer = getLinuxServerForServer(conn, mysqlServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.releaseConnection();
		Server.MasterStatus masterStatus = daemonConnector.getMySQLMasterStatus(mysqlServer);
		if(masterStatus==null) out.writeByte(AoservProtocol.DONE);
		else {
			out.writeByte(AoservProtocol.NEXT);
			out.writeNullUTF(masterStatus.getFile());
			out.writeNullUTF(masterStatus.getPosition());
		}
	}

	public static void getSlaveStatus(
		DatabaseConnection conn,
		RequestSource source,
		int failoverMySQLReplication,
		StreamableOutput out
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "getSlaveStatus", Permission.Name.get_mysql_slave_status);
		// Check access
		int mysqlServer = getServerForBackupMysqlReplication(conn, failoverMySQLReplication);
		checkAccessServer(conn, source, "getSlaveStatus", mysqlServer);
		int daemonServer;
		PosixPath chrootPath;
		int osv;
		if(conn.executeBooleanQuery("select ao_server is not null from backup.\"MysqlReplication\" where id=?", failoverMySQLReplication)) {
			// ao_server-based
			daemonServer = conn.executeIntQuery("select ao_server from backup.\"MysqlReplication\" where id=?", failoverMySQLReplication);
			chrootPath = null;
			osv = NetHostHandler.getOperatingSystemVersionForHost(conn, daemonServer);
			if(osv==-1) throw new SQLException("Unknown operating_system_version for daemonServer: "+daemonServer);
		} else {
			// replication-based
			daemonServer = conn.executeIntQuery("select bp.ao_server from backup.\"MysqlReplication\" fmr inner join backup.\"FileReplication\" ffr on fmr.replication=ffr.id inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where fmr.id=?", failoverMySQLReplication);
			PosixPath toPath = conn.executeObjectQuery(ObjectFactories.posixPathFactory,
				"select bp.path from backup.\"MysqlReplication\" fmr inner join backup.\"FileReplication\" ffr on fmr.replication=ffr.id inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where fmr.id=?",
				failoverMySQLReplication
			);
			int linuxServer = getLinuxServerForServer(conn, mysqlServer);
			osv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
			if(osv==-1) throw new SQLException("Unknown operating_system_version for linuxServer: "+linuxServer);
			try {
				chrootPath = PosixPath.valueOf(toPath+"/"+NetHostHandler.getHostnameForLinuxServer(conn, linuxServer));
			} catch(ValidationException e) {
				throw new SQLException(e);
			}
		}
		Tuple2<Server.Name,Port> serverNameAndPort = getNameAndPortForServer(conn, mysqlServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, daemonServer);
		conn.releaseConnection();
		MysqlReplication.SlaveStatus slaveStatus = daemonConnector.getMySQLSlaveStatus(chrootPath, osv, serverNameAndPort.getElement1(), serverNameAndPort.getElement2());
		if(slaveStatus==null) out.writeByte(AoservProtocol.DONE);
		else {
			out.writeByte(AoservProtocol.NEXT);
			out.writeNullUTF(slaveStatus.getSlaveIOState());
			out.writeNullUTF(slaveStatus.getMasterLogFile());
			out.writeNullUTF(slaveStatus.getReadMasterLogPos());
			out.writeNullUTF(slaveStatus.getRelayLogFile());
			out.writeNullUTF(slaveStatus.getRelayLogPos());
			out.writeNullUTF(slaveStatus.getRelayMasterLogFile());
			out.writeNullUTF(slaveStatus.getSlaveIORunning());
			out.writeNullUTF(slaveStatus.getSlaveSQLRunning());
			out.writeNullUTF(slaveStatus.getLastErrno());
			out.writeNullUTF(slaveStatus.getLastError());
			out.writeNullUTF(slaveStatus.getSkipCounter());
			out.writeNullUTF(slaveStatus.getExecMasterLogPos());
			out.writeNullUTF(slaveStatus.getRelayLogSpace());
			out.writeNullUTF(slaveStatus.getSecondsBehindMaster());
		}
	}

	public static void getTableStatus(
		DatabaseConnection conn,
		RequestSource source,
		int database,
		int mysqlSlave,
		StreamableOutput out
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "getTableStatus", Permission.Name.get_mysql_table_status);
		// Check access
		checkAccessDatabase(conn, source, "getTableStatus", database);
		int daemonServer;
		PosixPath chrootPath;
		int osv;
		int mysqlServer = getServerForDatabase(conn, database);
		if(mysqlSlave == -1) {
			// Query the master
			daemonServer = getLinuxServerForServer(conn, mysqlServer);
			chrootPath = null;
			osv = NetHostHandler.getOperatingSystemVersionForHost(conn, daemonServer);
			if(osv==-1) throw new SQLException("Unknown operating_system_version for daemonServer: " + daemonServer);
		} else {
			// Query the slave
			int slaveServer = getServerForBackupMysqlReplication(conn, mysqlSlave);
			if(slaveServer!=mysqlServer) throw new SQLException("slaveServer != server");
			if(conn.executeBooleanQuery("select ao_server is not null from backup.\"MysqlReplication\" where id=?", mysqlSlave)) {
				// ao_server-based
				daemonServer = conn.executeIntQuery("select ao_server from backup.\"MysqlReplication\" where id=?", mysqlSlave);
				chrootPath = null;
				osv = NetHostHandler.getOperatingSystemVersionForHost(conn, daemonServer);
				if(osv==-1) throw new SQLException("Unknown operating_system_version for daemonServer: " + daemonServer);
			} else {
				// replication-based
				daemonServer = conn.executeIntQuery("select bp.ao_server from backup.\"MysqlReplication\" fmr inner join backup.\"FileReplication\" ffr on fmr.replication=ffr.id inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where fmr.id=?", mysqlSlave);
				PosixPath toPath = conn.executeObjectQuery(ObjectFactories.posixPathFactory,
					"select bp.path from backup.\"MysqlReplication\" fmr inner join backup.\"FileReplication\" ffr on fmr.replication=ffr.id inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where fmr.id=?",
					mysqlSlave
				);
				int linuxServer = getLinuxServerForServer(conn, slaveServer);
				osv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
				if(osv==-1) throw new SQLException("Unknown operating_system_version for linuxServer: " + linuxServer);
				try {
					chrootPath = PosixPath.valueOf(toPath+"/"+NetHostHandler.getHostnameForLinuxServer(conn, linuxServer));
				} catch(ValidationException e) {
					throw new SQLException(e);
				}
			}
		}
		Tuple2<Server.Name,Port> serverNameAndPort = getNameAndPortForServer(conn, mysqlServer);
		Database.Name databaseName = getNameForDatabase(conn, database);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, daemonServer);
		conn.releaseConnection();
		List<Database.TableStatus> tableStatuses = daemonConnector.getMySQLTableStatus(chrootPath, osv, serverNameAndPort.getElement1(), serverNameAndPort.getElement2(), databaseName);
		out.writeByte(AoservProtocol.NEXT);
		int size = tableStatuses.size();
		out.writeCompressedInt(size);
		for(int c=0;c<size;c++) {
			Database.TableStatus tableStatus = tableStatuses.get(c);
			out.writeUTF(tableStatus.getName().toString());
			out.writeNullEnum(tableStatus.getEngine());
			out.writeNullInteger(tableStatus.getVersion());
			out.writeNullEnum(tableStatus.getRowFormat());
			out.writeNullLong(tableStatus.getRows());
			out.writeNullLong(tableStatus.getAvgRowLength());
			out.writeNullLong(tableStatus.getDataLength());
			out.writeNullLong(tableStatus.getMaxDataLength());
			out.writeNullLong(tableStatus.getIndexLength());
			out.writeNullLong(tableStatus.getDataFree());
			out.writeNullLong(tableStatus.getAutoIncrement());
			out.writeNullUTF(tableStatus.getCreateTime());
			out.writeNullUTF(tableStatus.getUpdateTime());
			out.writeNullUTF(tableStatus.getCheckTime());
			out.writeNullEnum(tableStatus.getCollation());
			out.writeNullUTF(tableStatus.getChecksum());
			out.writeNullUTF(tableStatus.getCreateOptions());
			out.writeNullUTF(tableStatus.getComment());
		}
	}

	public static void checkTables(
		DatabaseConnection conn,
		RequestSource source,
		int database,
		int mysqlSlave,
		List<Table_Name> tableNames,
		StreamableOutput out
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "checkTables", Permission.Name.check_mysql_tables);
		// Check access
		checkAccessDatabase(conn, source, "checkTables", database);
		int daemonServer;
		PosixPath chrootPath;
		int osv;
		int mysqlServer = getServerForDatabase(conn, database);
		if(mysqlSlave==-1) {
			// Query the master
			daemonServer = getLinuxServerForServer(conn, mysqlServer);
			chrootPath = null;
			osv = NetHostHandler.getOperatingSystemVersionForHost(conn, daemonServer);
			if(osv==-1) throw new SQLException("Unknown operating_system_version for daemonServer: " + daemonServer);
		} else {
			// Query the slave
			int slaveServer = getServerForBackupMysqlReplication(conn, mysqlSlave);
			if(slaveServer != mysqlServer) throw new SQLException("slaveServer != server");
			if(conn.executeBooleanQuery("select ao_server is not null from backup.\"MysqlReplication\" where id=?", mysqlSlave)) {
				// ao_server-based
				daemonServer = conn.executeIntQuery("select ao_server from backup.\"MysqlReplication\" where id=?", mysqlSlave);
				chrootPath = null;
				osv = NetHostHandler.getOperatingSystemVersionForHost(conn, daemonServer);
				if(osv==-1) throw new SQLException("Unknown operating_system_version for daemonServer: " + daemonServer);
			} else {
				// replication-based
				daemonServer = conn.executeIntQuery("select bp.ao_server from backup.\"MysqlReplication\" fmr inner join backup.\"FileReplication\" ffr on fmr.replication=ffr.id inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where fmr.id=?", mysqlSlave);
				PosixPath toPath = conn.executeObjectQuery(ObjectFactories.posixPathFactory,
					"select bp.path from backup.\"MysqlReplication\" fmr inner join backup.\"FileReplication\" ffr on fmr.replication=ffr.id inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where fmr.id=?",
					mysqlSlave
				);
				int linuxServer = getLinuxServerForServer(conn, slaveServer);
				osv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
				if(osv==-1) throw new SQLException("Unknown operating_system_version for linuxServer: " + linuxServer);
				try {
					chrootPath = PosixPath.valueOf(toPath+"/"+NetHostHandler.getHostnameForLinuxServer(conn, linuxServer));
				} catch(ValidationException e) {
					throw new SQLException(e);
				}
			}
		}
		Tuple2<Server.Name,Port> serverNameAndPort = getNameAndPortForServer(conn, mysqlServer);
		Database.Name databaseName = getNameForDatabase(conn, database);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, daemonServer);
		conn.releaseConnection();
		List<Database.CheckTableResult> checkTableResults = daemonConnector.checkMySQLTables(chrootPath, osv, serverNameAndPort.getElement1(), serverNameAndPort.getElement2(), databaseName, tableNames);
		out.writeByte(AoservProtocol.NEXT);
		int size = checkTableResults.size();
		out.writeCompressedInt(size);
		for(int c=0;c<size;c++) {
			Database.CheckTableResult checkTableResult = checkTableResults.get(c);
			out.writeUTF(checkTableResult.getTable().toString());
			out.writeLong(checkTableResult.getDuration());
			out.writeNullEnum(checkTableResult.getMsgType());
			out.writeNullUTF(checkTableResult.getMsgText());
		}
	}
}
