/*
 * Copyright 2001-2013, 2015, 2016, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.backup.MysqlReplication;
import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.mysql.Database;
import com.aoindustries.aoserv.client.mysql.Server;
import com.aoindustries.aoserv.client.mysql.UserServer;
import com.aoindustries.aoserv.client.password.PasswordChecker;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.MySQLDatabaseName;
import com.aoindustries.aoserv.client.validator.MySQLTableName;
import com.aoindustries.aoserv.client.validator.MySQLUserId;
import com.aoindustries.aoserv.client.validator.UnixPath;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.net.Port;
import com.aoindustries.util.IntList;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The <code>MySQLHandler</code> handles all the accesses to the MySQL tables.
 *
 * @author  AO Industries, Inc.
 */
final public class MySQLHandler {

	private MySQLHandler() {
	}

	private final static Map<Integer,Boolean> disabledMySQLServerUsers=new HashMap<>();
	private final static Map<MySQLUserId,Boolean> disabledMySQLUsers=new HashMap<>();

	public static void checkAccessMySQLDatabase(DatabaseConnection conn, RequestSource source, String action, int mysql_database) throws IOException, SQLException {
		User mu = MasterServer.getUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getUsername()).length!=0) {
				int mysqlServer=getMySQLServerForMySQLDatabase(conn, mysql_database);
				int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
				ServerHandler.checkAccessServer(conn, source, action, aoServer);
			}
		} else {
			PackageHandler.checkAccessPackage(conn, source, action, getPackageForMySQLDatabase(conn, mysql_database));
		}
	}

	public static void checkAccessMySQLDBUser(DatabaseConnection conn, RequestSource source, String action, int id) throws IOException, SQLException {
		checkAccessMySQLDatabase(conn, source, action, getDatabaseForDatabaseUser(conn, id));
		checkAccessMySQLServerUser(conn, source, action, getUserServerForDatabaseUser(conn, id));
	}

	public static void checkAccessMySQLServerUser(DatabaseConnection conn, RequestSource source, String action, int mysql_server_user) throws IOException, SQLException {
		User mu = MasterServer.getUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getUsername()).length!=0) {
				int mysqlServer = getMySQLServerForMySQLServerUser(conn, mysql_server_user);
				int aoServer = getAOServerForMySQLServer(conn, mysqlServer);
				ServerHandler.checkAccessServer(conn, source, action, aoServer);
			}
		} else {
			checkAccessMySQLUser(conn, source, action, getUsernameForMySQLServerUser(conn, mysql_server_user));
		}
	}

	public static void checkAccessMySQLServer(DatabaseConnection conn, RequestSource source, String action, int mysql_server) throws IOException, SQLException {
		User mu = MasterServer.getUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getUsername()).length!=0) {
				// Protect by server
				int aoServer = getAOServerForMySQLServer(conn, mysql_server);
				ServerHandler.checkAccessServer(conn, source, action, aoServer);
			}
		} else {
			// Protect by package
			AccountingCode packageName = getPackageForMySQLServer(conn, mysql_server);
			PackageHandler.checkAccessPackage(conn, source, action, packageName);
		}
	}

	public static void checkAccessMySQLUser(DatabaseConnection conn, RequestSource source, String action, MySQLUserId username) throws IOException, SQLException {
		User mu = MasterServer.getUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getUsername()).length!=0) {
				IntList msus = getMySQLServerUsersForMySQLUser(conn, username);
				boolean found = false;
				for(int msu : msus) {
					int mysqlServer = getMySQLServerForMySQLServerUser(conn, msu);
					int aoServer = getAOServerForMySQLServer(conn, mysqlServer);
					if(ServerHandler.canAccessServer(conn, source, aoServer)) {
						found=true;
						break;
					}
				}
				if(!found) {
					String message=
						"business_administrator.username="
						+source.getUsername()
						+" is not allowed to access mysql_user: action='"
						+action
						+", username="
						+username
					;
					throw new SQLException(message);
				}
			}
		} else {
			UsernameHandler.checkAccessUsername(conn, source, action, username);
		}
	}

	/**
	 * Adds a MySQL database to the system.
	 */
	public static int addMySQLDatabase(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		MySQLDatabaseName name,
		int mysqlServer,
		AccountingCode packageName
	) throws IOException, SQLException {
		int aoServer=getAOServerForMySQLServer(conn, mysqlServer);

		PackageHandler.checkPackageAccessServer(conn, source, "addMySQLDatabase", packageName, aoServer);
		if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add Database '"+name+"', Package disabled: "+packageName);

		// Must be allowed to access this server and package
		ServerHandler.checkAccessServer(conn, source, "addMySQLDatabase", aoServer);
		PackageHandler.checkAccessPackage(conn, source, "addMySQLDatabase", packageName);

		// Find the accouting code
		AccountingCode accounting=PackageHandler.getBusinessForPackage(conn, packageName);
		// This sub-account must have access to the server
		BusinessHandler.checkBusinessAccessServer(conn, source, "addMySQLDatabase", accounting, aoServer);

		// Add the entry to the database
		int id = conn.executeIntUpdate(
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
			accounting,
			aoServer,
			false
		);
		return id;
	}

	/**
	 * Grants a UserServer access to a MySQLMasterDatabase.getDatabase().
	 */
	public static int addMySQLDBUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int mysql_database,
		int mysql_server_user,
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
		checkAccessMySQLDatabase(conn, source, "addMySQLDBUser", mysql_database);
		checkAccessMySQLServerUser(conn, source, "addMySQLDBUser", mysql_server_user);
		if(isMySQLServerUserDisabled(conn, mysql_server_user)) throw new SQLException("Unable to add DatabaseUser, UserServer disabled: "+mysql_server_user);

		// Must also have matching servers
		int dbServer=getMySQLServerForMySQLDatabase(conn, mysql_database);
		int userServer=getMySQLServerForMySQLServerUser(conn, mysql_server_user);
		// TODO: Enforce this with PostgreSQL trigger
		if(dbServer!=userServer) throw new SQLException("Mismatched mysql.Server for mysql.Database and mysql.UserServer");

		// Add the entry to the database
		int id = conn.executeIntUpdate(
			"INSERT INTO mysql.\"DatabaseUser\" VALUES (default,?,?,?,?,?,?,?,?,false,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id",
			mysql_database,
			mysql_server_user,
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
			getBusinessForMySQLServerUser(conn, mysql_server_user),
			getAOServerForMySQLServer(conn, dbServer),
			false
		);
		return id;
	}

	/**
	 * Adds a MySQL server user.
	 */
	public static int addMySQLServerUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		MySQLUserId username,
		int mysqlServer,
		String host
	) throws IOException, SQLException {
		checkAccessMySQLUser(conn, source, "addMySQLServerUser", username);
		if(isMySQLUserDisabled(conn, username)) throw new SQLException("Unable to add UserServer, User disabled: "+username);
		if(username.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) throw new SQLException("Not allowed to add UserServer for user '"+com.aoindustries.aoserv.client.linux.User.MAIL+'\'');
		int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
		ServerHandler.checkAccessServer(conn, source, "addMySQLServerUser", aoServer);
		// This sub-account must have access to the server
		UsernameHandler.checkUsernameAccessServer(conn, source, "addMySQLServerUser", username, aoServer);

		boolean isSystemUser = username.equals(com.aoindustries.aoserv.client.mysql.User.ROOT) || username.equals(com.aoindustries.aoserv.client.mysql.User.MYSQL_SYS);
		int id = conn.executeIntUpdate(
			"INSERT INTO mysql.\"UserServer\" VALUES(default,?,?,?,null,null,?,?,?,?) RETURNING id",
			username,
			mysqlServer,
			host,
			isSystemUser ? UserServer.UNLIMITED_QUESTIONS        : UserServer.DEFAULT_MAX_QUESTIONS,
			isSystemUser ? UserServer.UNLIMITED_UPDATES          : UserServer.DEFAULT_MAX_UPDATES,
			isSystemUser ? UserServer.UNLIMITED_CONNECTIONS      : UserServer.DEFAULT_MAX_CONNECTIONS,
			isSystemUser ? UserServer.UNLIMITED_USER_CONNECTIONS : UserServer.DEFAULT_MAX_USER_CONNECTIONS
		);

		// Notify all clients of the update
		AccountingCode accounting = UsernameHandler.getBusinessForUsername(conn, username);
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_SERVER_USERS,
			accounting,
			aoServer,
			true
		);
		return id;
	}

	/**
	 * Adds a MySQL user.
	 */
	public static void addMySQLUser(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		MySQLUserId username
	) throws IOException, SQLException {
		UsernameHandler.checkAccessUsername(conn, source, "addMySQLUser", username);
		if(UsernameHandler.isUsernameDisabled(conn, username)) throw new SQLException("Unable to add User, Username disabled: "+username);
		if(username.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) throw new SQLException("Not allowed to add User for user '"+com.aoindustries.aoserv.client.linux.User.MAIL+'\'');

		conn.executeUpdate(
			"insert into mysql.\"User\"(username) values(?)",
			username
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_USERS,
			UsernameHandler.getBusinessForUsername(conn, username),
			InvalidateList.allServers,
			true
		);
	}

	public static void disableMySQLServerUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		int id
	) throws IOException, SQLException {
		if(isMySQLServerUserDisabled(conn, id)) throw new SQLException("UserServer is already disabled: "+id);
		BusinessHandler.checkAccessDisableLog(conn, source, "disableMySQLServerUser", disableLog, false);
		checkAccessMySQLServerUser(conn, source, "disableMySQLServerUser", id);

		conn.executeUpdate(
			"update mysql.\"UserServer\" set disable_log=? where id=?",
			disableLog,
			id
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_SERVER_USERS,
			getBusinessForMySQLServerUser(conn, id),
			getAOServerForMySQLServer(conn, getMySQLServerForMySQLServerUser(conn, id)),
			false
		);
	}

	public static void disableMySQLUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		MySQLUserId username
	) throws IOException, SQLException {
		if(isMySQLUserDisabled(conn, username)) throw new SQLException("User is already disabled: "+username);
		BusinessHandler.checkAccessDisableLog(conn, source, "disableMySQLUser", disableLog, false);
		checkAccessMySQLUser(conn, source, "disableMySQLUser", username);
		IntList msus=getMySQLServerUsersForMySQLUser(conn, username);
		for(int c=0;c<msus.size();c++) {
			int msu=msus.getInt(c);
			if(!isMySQLServerUserDisabled(conn, msu)) {
				throw new SQLException("Cannot disable User '"+username+"': UserServer not disabled: "+msu);
			}
		}

		conn.executeUpdate(
			"update mysql.\"User\" set disable_log=? where username=?",
			disableLog,
			username
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_USERS,
			UsernameHandler.getBusinessForUsername(conn, username),
			UsernameHandler.getServersForUsername(conn, username),
			false
		);
	}

	/**
	 * Dumps a MySQL database
	 */
	public static void dumpMySQLDatabase(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataOutputStream out,
		int dbPKey,
		boolean gzip
	) throws IOException, SQLException {
		checkAccessMySQLDatabase(conn, source, "dumpMySQLDatabase", dbPKey);

		int mysqlServer=getMySQLServerForMySQLDatabase(conn, dbPKey);
		int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).dumpMySQLDatabase(
			dbPKey,
			gzip,
			(long dumpSize) -> {
				if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_0) >= 0) {
					out.writeLong(dumpSize);
				}
			},
			out
		);
	}

	public static void enableMySQLServerUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id
	) throws IOException, SQLException {
		int disableLog=getDisableLogForMySQLServerUser(conn, id);
		if(disableLog==-1) throw new SQLException("UserServer is already enabled: "+id);
		BusinessHandler.checkAccessDisableLog(conn, source, "enableMySQLServerUser", disableLog, true);
		checkAccessMySQLServerUser(conn, source, "enableMySQLServerUser", id);
		MySQLUserId mu=getUsernameForMySQLServerUser(conn, id);
		if(isMySQLUserDisabled(conn, mu)) throw new SQLException("Unable to enable UserServer #"+id+", User not enabled: "+mu);

		conn.executeUpdate(
			"update mysql.\"UserServer\" set disable_log=null where id=?",
			id
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_SERVER_USERS,
			UsernameHandler.getBusinessForUsername(conn, mu),
			getAOServerForMySQLServer(conn, getMySQLServerForMySQLServerUser(conn, id)),
			false
		);
	}

	public static void enableMySQLUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		MySQLUserId username
	) throws IOException, SQLException {
		int disableLog=getDisableLogForMySQLUser(conn, username);
		if(disableLog==-1) throw new SQLException("User is already enabled: "+username);
		BusinessHandler.checkAccessDisableLog(conn, source, "enableMySQLUser", disableLog, true);
		UsernameHandler.checkAccessUsername(conn, source, "enableMySQLUser", username);
		if(UsernameHandler.isUsernameDisabled(conn, username)) throw new SQLException("Unable to enable User '"+username+"', Username not enabled: "+username);

		conn.executeUpdate(
			"update mysql.\"User\" set disable_log=null where username=?",
			username
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_USERS,
			UsernameHandler.getBusinessForUsername(conn, username),
			UsernameHandler.getServersForUsername(conn, username),
			false
		);
	}

	/**
	 * Generates a unique MySQL database name.
	 */
	public static MySQLDatabaseName generateMySQLDatabaseName(
		DatabaseConnection conn,
		String template_base,
		String template_added
	) throws IOException, SQLException {
		// Load the entire list of mysql database names
		Set<MySQLDatabaseName> names = conn.executeObjectCollectionQuery(
			new HashSet<>(),
			ObjectFactories.mySQLDatabaseNameFactory,
			"select name from mysql.\"Database\" group by name"
		);
		// Find one that is not used
		for(int c=0;c<Integer.MAX_VALUE;c++) {
			MySQLDatabaseName name;
			try {
				name = MySQLDatabaseName.valueOf((c==0) ? template_base : (template_base+template_added+c));
			} catch(ValidationException e) {
				throw new SQLException(e.getLocalizedMessage(), e);
			}
			if(!names.contains(name)) return name;
		}
		// If could not find one, report and error
		throw new SQLException("Unable to find available MySQL database name for template_base="+template_base+" and template_added="+template_added);
	}

	public static int getDisableLogForMySQLServerUser(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from mysql.\"UserServer\" where id=?", id);
	}

	public static int getDisableLogForMySQLUser(DatabaseConnection conn, MySQLUserId username) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from mysql.\"User\" where username=?", username);
	}

	public static MySQLUserId getUsernameForMySQLServerUser(DatabaseConnection conn, int msu) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.mySQLUserIdFactory,
			"select username from mysql.\"UserServer\" where id=?",
			msu
		);
	}

	public static MySQLDatabaseName getMySQLDatabaseName(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.mySQLDatabaseNameFactory,
			"select name from mysql.\"Database\" where id=?",
			id
		);
	}

	public static void invalidateTable(Table.TableID tableID) {
		switch(tableID) {
			case MYSQL_SERVER_USERS :
				synchronized(MySQLHandler.class) {
					disabledMySQLServerUsers.clear();
				}
				break;
			case MYSQL_USERS :
				synchronized(MySQLHandler.class) {
					disabledMySQLUsers.clear();
				}
				break;
		}
	}

	public static boolean isMySQLServerUserDisabled(DatabaseConnection conn, int id) throws IOException, SQLException {
		synchronized(MySQLHandler.class) {
			Integer I=id;
			Boolean O=disabledMySQLServerUsers.get(I);
			if(O!=null) return O;
			boolean isDisabled=getDisableLogForMySQLServerUser(conn, id)!=-1;
			disabledMySQLServerUsers.put(I, isDisabled);
			return isDisabled;
		}
	}

	public static boolean isMySQLUser(DatabaseConnection conn, UserId username) throws IOException, SQLException {
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
			username
		);
	}

	public static boolean isMySQLUserDisabled(DatabaseConnection conn, MySQLUserId username) throws IOException, SQLException {
		synchronized(MySQLHandler.class) {
			Boolean O=disabledMySQLUsers.get(username);
			if(O!=null) return O;
			boolean isDisabled=getDisableLogForMySQLUser(conn, username)!=-1;
			disabledMySQLUsers.put(username, isDisabled);
			return isDisabled;
		}
	}

	/**
	 * Determines if a MySQL database name is available.
	 */
	public static boolean isMySQLDatabaseNameAvailable(
		DatabaseConnection conn,
		RequestSource source,
		MySQLDatabaseName name,
		int mysqlServer
	) throws IOException, SQLException {
		int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
		ServerHandler.checkAccessServer(conn, source, "isMySQLDatabaseNameAvailable", aoServer);
		return conn.executeBooleanQuery("select (select id from mysql.\"Database\" where name=? and mysql_server=?) is null", name, mysqlServer);
	}

	public static boolean isMySQLServerUserPasswordSet(
		DatabaseConnection conn,
		RequestSource source,
		int msu
	) throws IOException, SQLException {
		checkAccessMySQLServerUser(conn, source, "isMySQLServerUserPasswordSet", msu);
		if(isMySQLServerUserDisabled(conn, msu)) throw new SQLException("Unable to determine if the UserServer password is set, account disabled: "+msu);
		MySQLUserId username=getUsernameForMySQLServerUser(conn, msu);
		int mysqlServer=getMySQLServerForMySQLServerUser(conn, msu);
		int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
		String password=DaemonHandler.getDaemonConnector(conn, aoServer).getEncryptedMySQLUserPassword(mysqlServer, username);
		return !com.aoindustries.aoserv.client.mysql.User.NO_PASSWORD_DB_VALUE.equals(password);
	}

	/**
	 * Removes a Database from the system.
	 */
	public static void removeMySQLDatabase(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id
	) throws IOException, SQLException {
		checkAccessMySQLDatabase(conn, source, "removeMySQLDatabase", id);

		removeMySQLDatabase(conn, invalidateList, id);
	}

	/**
	 * Removes a Database from the system.
	 */
	public static void removeMySQLDatabase(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int id
	) throws IOException, SQLException {
		// Cannot remove the mysql database
		MySQLDatabaseName dbName = getMySQLDatabaseName(conn, id);
		if(
			dbName.equals(Database.MYSQL)
			|| dbName.equals(Database.INFORMATION_SCHEMA)
			|| dbName.equals(Database.PERFORMANCE_SCHEMA)
			|| dbName.equals(Database.SYS)
		) {
			throw new SQLException("Not allowed to remove the database named '" + dbName + '\'');
		}

		// Remove the mysql_db_user entries
		List<AccountingCode> dbUserAccounts=conn.executeObjectCollectionQuery(
			new ArrayList<AccountingCode>(),
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  mysql.\"DatabaseUser\" mdu,\n"
			+ "  mysql.\"UserServer\" msu,\n"
			+ "  account.\"Username\" un,\n"
			+ "  billing.\"Package\" pk\n"
			+ "where\n"
			+ "  mdu.mysql_database=?\n"
			+ "  and mdu.mysql_server_user=msu.id\n"
			+ "  and msu.username=un.username\n"
			+ "  and un.package=pk.name\n"
			+ "group by\n"
			+ "  pk.accounting",
			id
		);
		if(dbUserAccounts.size()>0) conn.executeUpdate("delete from mysql.\"DatabaseUser\" where mysql_database=?", id);

		// Remove the database entry
		AccountingCode accounting = getBusinessForMySQLDatabase(conn, id);
		int mysqlServer=getMySQLServerForMySQLDatabase(conn, id);
		int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
		conn.executeUpdate("delete from mysql.\"Database\" where id=?", id);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_DATABASES,
			accounting,
			aoServer,
			false
		);
		if(dbUserAccounts.size()>0) invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_DB_USERS,
			dbUserAccounts,
			aoServer,
			false
		);
	}

	/**
	 * Removes a DatabaseUser from the system.
	 */
	public static void removeMySQLDBUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id
	) throws IOException, SQLException {
		checkAccessMySQLDBUser(conn, source, "removeMySQLDBUser", id);

		// Remove the mysql_db_user
		AccountingCode accounting = getBusinessForMySQLDBUser(conn, id);
		int mysqlServer=getMysqlServerForDatabaseUser(conn, id);
		int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
		conn.executeUpdate("delete from mysql.\"DatabaseUser\" where id=?", id);

		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_DB_USERS,
			accounting,
			aoServer,
			false
		);
	}

	/**
	 * Removes a UserServer from the system.
	 */
	public static void removeMySQLServerUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id
	) throws IOException, SQLException {
		checkAccessMySQLServerUser(conn, source, "removeMySQLServerUser", id);

		MySQLUserId username=getUsernameForMySQLServerUser(conn, id);
		if(
			username.equals(com.aoindustries.aoserv.client.mysql.User.ROOT)
			|| username.equals(com.aoindustries.aoserv.client.mysql.User.MYSQL_SYS)
		) throw new SQLException("Not allowed to remove UserServer for user '" + username + '\'');

		// Remove the mysql_db_user
		boolean dbUsersExist=conn.executeBooleanQuery("select (select id from mysql.\"DatabaseUser\" where mysql_server_user=? limit 1) is not null", id);
		if(dbUsersExist) conn.executeUpdate("delete from mysql.\"DatabaseUser\" where mysql_server_user=?", id);

		// Remove the mysql_server_user
		AccountingCode accounting = getBusinessForMySQLServerUser(conn, id);
		int mysqlServer=getMySQLServerForMySQLServerUser(conn, id);
		int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
		conn.executeUpdate("delete from mysql.\"UserServer\" where id=?", id);

		// Notify all clients of the updates
		if(dbUsersExist) invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_DB_USERS,
			accounting,
			aoServer,
			false
		);
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_SERVER_USERS,
			accounting,
			aoServer,
			true
		);
	}

	/**
	 * Removes a User from the system.
	 */
	public static void removeMySQLUser(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		MySQLUserId username
	) throws IOException, SQLException {
		checkAccessMySQLUser(conn, source, "removeMySQLUser", username);

		removeMySQLUser(conn, invalidateList, username);
	}

	/**
	 * Removes a User from the system.
	 */
	public static void removeMySQLUser(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		MySQLUserId username
	) throws IOException, SQLException {
		if(
			username.equals(com.aoindustries.aoserv.client.mysql.User.ROOT)
			|| username.equals(com.aoindustries.aoserv.client.mysql.User.MYSQL_SYS)
		) throw new SQLException("Not allowed to remove User for user '" + username + '\'');

		AccountingCode accounting = UsernameHandler.getBusinessForUsername(conn, username);

		// Remove the mysql_db_user
		IntList dbUserServers=conn.executeIntListQuery(
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
			username
		);
		if(dbUserServers.size()>0) {
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
				username
			);
			for(int mysqlServer : dbUserServers) {
				invalidateList.addTable(
					conn,
					Table.TableID.MYSQL_DB_USERS,
					accounting,
					getAOServerForMySQLServer(conn, mysqlServer),
					false
				);
			}
		}

		// Remove the mysql_server_user
		IntList mysqlServers=conn.executeIntListQuery("select mysql_server from mysql.\"UserServer\" where username=?", username);
		if(mysqlServers.size()>0) {
			conn.executeUpdate("delete from mysql.\"UserServer\" where username=?", username);
			for(int mysqlServer : mysqlServers) {
				invalidateList.addTable(
					conn,
					Table.TableID.MYSQL_SERVER_USERS,
					accounting,
					getAOServerForMySQLServer(conn, mysqlServer),
					false
				);
			}
		}

		// Remove the mysql_user
		conn.executeUpdate("delete from mysql.\"User\" where username=?", username);
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_USERS,
			accounting,
			BusinessHandler.getServersForBusiness(conn, accounting),
			false
		);
	}

	/**
	 * Sets a MySQL password.
	 */
	public static void setMySQLServerUserPassword(
		DatabaseConnection conn,
		RequestSource source,
		int mysql_server_user,
		String password
	) throws IOException, SQLException {
		BusinessHandler.checkPermission(conn, source, "setMySQLServerUserPassword", Permission.Name.set_mysql_server_user_password);
		checkAccessMySQLServerUser(conn, source, "setMySQLServerUserPassword", mysql_server_user);
		if(isMySQLServerUserDisabled(conn, mysql_server_user)) throw new SQLException("Unable to set UserServer password, account disabled: "+mysql_server_user);

		// Get the server, username for the user
		MySQLUserId username=getUsernameForMySQLServerUser(conn, mysql_server_user);

		// No setting the super user password
		if(
			username.equals(com.aoindustries.aoserv.client.mysql.User.ROOT)
			|| username.equals(com.aoindustries.aoserv.client.mysql.User.MYSQL_SYS)
		) throw new SQLException("The MySQL " + username + " password may not be set.");

		// Perform the password check here, too.
		if(password!=null && password.length()==0) password=com.aoindustries.aoserv.client.mysql.User.NO_PASSWORD;
		if(password!=com.aoindustries.aoserv.client.mysql.User.NO_PASSWORD) {
			List<PasswordChecker.Result> results = com.aoindustries.aoserv.client.mysql.User.checkPassword(username, password);
			if(PasswordChecker.hasResults(results)) throw new SQLException("Invalid password: "+PasswordChecker.getResultsString(results).replace('\n', '|'));
		}

		// Contact the daemon for the update
		int mysqlServer=getMySQLServerForMySQLServerUser(conn, mysql_server_user);
		int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
		DaemonHandler.getDaemonConnector(
			conn,
			aoServer
		).setMySQLUserPassword(mysqlServer, username, password);
	}

	public static void setMySQLServerUserPredisablePassword(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int msu,
		String password
	) throws IOException, SQLException {
		checkAccessMySQLServerUser(conn, source, "setMySQLServerUserPredisablePassword", msu);
		if(password==null) {
			if(isMySQLServerUserDisabled(conn, msu)) throw new SQLException("Unable to clear UserServer predisable password, account disabled: "+msu);
		} else {
			if(!isMySQLServerUserDisabled(conn, msu)) throw new SQLException("Unable to set UserServer predisable password, account not disabled: "+msu);
		}

		// Update the database
		conn.executeUpdate(
			"update mysql.\"UserServer\" set predisable_password=? where id=?",
			password,
			msu
		);

		int mysqlServer=getMySQLServerForMySQLServerUser(conn, msu);
		int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
		invalidateList.addTable(
			conn,
			Table.TableID.MYSQL_SERVER_USERS,
			getBusinessForMySQLServerUser(conn, msu),
			aoServer,
			false
		);
	}

	/**
	 * Waits for any pending or processing MySQL database config rebuild to complete.
	 */
	public static void waitForMySQLDatabaseRebuild(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "waitForMySQLDatabaseRebuild", aoServer);
		ServerHandler.waitForInvalidates(aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).waitForMySQLDatabaseRebuild();
	}

	/**
	 * Waits for any pending or processing MySQL database config rebuild to complete.
	 */
	public static void waitForMySQLDBUserRebuild(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "waitForMySQLDBUserRebuild", aoServer);
		ServerHandler.waitForInvalidates(aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).waitForMySQLDBUserRebuild();
	}

	public static void waitForMySQLServerRebuild(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "waitForMySQLServerRebuild", aoServer);
		ServerHandler.waitForInvalidates(aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).waitForMySQLServerRebuild();
	}

	/**
	 * Waits for any pending or processing MySQL database config rebuild to complete.
	 */
	public static void waitForMySQLUserRebuild(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "waitForMySQLUserRebuild", aoServer);
		ServerHandler.waitForInvalidates(aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).waitForMySQLUserRebuild();
	}

	public static AccountingCode getBusinessForMySQLDatabase(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select pk.accounting from mysql.\"Database\" md, billing.\"Package\" pk where md.package=pk.name and md.id=?",
			id
		);
	}

	public static AccountingCode getBusinessForMySQLDBUser(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  mysql.\"DatabaseUser\" mdu,\n"
			+ "  mysql.\"UserServer\" msu,\n"
			+ "  account.\"Username\" un,\n"
			+ "  billing.\"Package\" pk\n"
			+ "where\n"
			+ "  mdu.id=?\n"
			+ "  and mdu.mysql_server_user=msu.id\n"
			+ "  and msu.username=un.username\n"
			+ "  and un.package=pk.name",
			id
		);
	}

	public static AccountingCode getBusinessForMySQLServerUser(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  mysql.\"UserServer\" msu,\n"
			+ "  account.\"Username\" un,\n"
			+ "  billing.\"Package\" pk\n"
			+ "where\n"
			+ "  msu.username=un.username\n"
			+ "  and un.package=pk.name\n"
			+ "  and msu.id=?",
			id
		);
	}

	public static int getPackageForMySQLDatabase(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeIntQuery("select pk.id from mysql.\"Database\" md, billing.\"Package\" pk where md.id=? and md.package=pk.name", id);
	}

	public static IntList getMySQLServerUsersForMySQLUser(DatabaseConnection conn, MySQLUserId username) throws IOException, SQLException {
		return conn.executeIntListQuery("select id from mysql.\"UserServer\" where username=?", username);
	}

	public static int getMySQLServerForMySQLDatabase(DatabaseConnection conn, int mysql_database) throws IOException, SQLException {
		return conn.executeIntQuery("select mysql_server from mysql.\"Database\" where id=?", mysql_database);
	}

	public static int getAOServerForMySQLServer(DatabaseConnection conn, int mysqlServer) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from mysql.\"Server\" where bind=?", mysqlServer);
	}

	public static AccountingCode getPackageForMySQLServer(DatabaseConnection conn, int mysqlServer) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
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

	public static Port getPortForMySQLServer(DatabaseConnection conn, int mysqlServer) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.portFactory,
			"select\n"
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

	public static int getMySQLServerForFailoverMySQLReplication(DatabaseConnection conn, int failoverMySQLReplication) throws IOException, SQLException {
		return conn.executeIntQuery("select mysql_server from backup.\"MysqlReplication\" where id=?", failoverMySQLReplication);
	}

	public static int getMysqlServerForDatabaseUser(DatabaseConnection conn, int databaseUser) throws IOException, SQLException {
		return conn.executeIntQuery("select msu.mysql_server from mysql.\"DatabaseUser\" mdu, mysql.\"UserServer\" msu where mdu.id=? and mdu.mysql_server_user=msu.id", databaseUser);
	}

	public static int getDatabaseForDatabaseUser(DatabaseConnection conn, int databaseUser) throws IOException, SQLException {
		return conn.executeIntQuery("select mysql_database from mysql.\"DatabaseUser\" where id=?", databaseUser);
	}

	public static int getUserServerForDatabaseUser(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeIntQuery("select mysql_server_user from mysql.\"DatabaseUser\" where id=?", id);
	}

	public static int getMySQLServerForMySQLServerUser(DatabaseConnection conn, int mysql_server_user) throws IOException, SQLException {
		return conn.executeIntQuery("select mysql_server from mysql.\"UserServer\" where id=?", mysql_server_user);
	}

	public static void restartMySQL(
		DatabaseConnection conn,
		RequestSource source,
		int mysqlServer
	) throws IOException, SQLException {
		int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_mysql");
		if(!canControl) throw new SQLException("Not allowed to restart MySQL on "+aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).restartMySQL(mysqlServer);
	}

	public static void startMySQL(
		DatabaseConnection conn,
		RequestSource source,
		int mysqlServer
	) throws IOException, SQLException {
		int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_mysql");
		if(!canControl) throw new SQLException("Not allowed to start MySQL on "+aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).startMySQL(mysqlServer);
	}

	public static void stopMySQL(
		DatabaseConnection conn,
		RequestSource source,
		int mysqlServer
	) throws IOException, SQLException {
		int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_mysql");
		if(!canControl) throw new SQLException("Not allowed to stop MySQL on "+aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).stopMySQL(mysqlServer);
	}

	public static void getMasterStatus(
		DatabaseConnection conn,
		RequestSource source,
		int mysqlServer,
		CompressedDataOutputStream out
	) throws IOException, SQLException {
		BusinessHandler.checkPermission(conn, source, "getMasterStatus", Permission.Name.get_mysql_master_status);
		// Check access
		checkAccessMySQLServer(conn, source, "getMasterStatus", mysqlServer);
		int aoServer = getAOServerForMySQLServer(conn, mysqlServer);
		Server.MasterStatus masterStatus = DaemonHandler.getDaemonConnector(conn, aoServer).getMySQLMasterStatus(
			mysqlServer
		);
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
		CompressedDataOutputStream out
	) throws IOException, SQLException {
		BusinessHandler.checkPermission(conn, source, "getSlaveStatus", Permission.Name.get_mysql_slave_status);
		// Check access
		int mysqlServer = getMySQLServerForFailoverMySQLReplication(conn, failoverMySQLReplication);
		checkAccessMySQLServer(conn, source, "getSlaveStatus", mysqlServer);
		int daemonServer;
		UnixPath chrootPath;
		int osv;
		if(conn.executeBooleanQuery("select ao_server is not null from backup.\"MysqlReplication\" where id=?", failoverMySQLReplication)) {
			// ao_server-based
			daemonServer = conn.executeIntQuery("select ao_server from backup.\"MysqlReplication\" where id=?", failoverMySQLReplication);
			chrootPath = null;
			osv = ServerHandler.getOperatingSystemVersionForServer(conn, daemonServer);
			if(osv==-1) throw new SQLException("Unknown operating_system_version for aoServer: "+daemonServer);
		} else {
			// replication-based
			daemonServer = conn.executeIntQuery("select bp.ao_server from backup.\"MysqlReplication\" fmr inner join backup.\"FileReplication\" ffr on fmr.replication=ffr.id inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where fmr.id=?", failoverMySQLReplication);
			UnixPath toPath = conn.executeObjectQuery(
				ObjectFactories.unixPathFactory,
				"select bp.path from backup.\"MysqlReplication\" fmr inner join backup.\"FileReplication\" ffr on fmr.replication=ffr.id inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where fmr.id=?",
				failoverMySQLReplication
			);
			int aoServer = getAOServerForMySQLServer(conn, mysqlServer);
			osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
			if(osv==-1) throw new SQLException("Unknown operating_system_version for aoServer: "+aoServer);
			try {
				chrootPath = UnixPath.valueOf(toPath+"/"+ServerHandler.getHostnameForAOServer(conn, aoServer));
			} catch(ValidationException e) {
				throw new SQLException(e);
			}
		}
		MysqlReplication.SlaveStatus slaveStatus = DaemonHandler.getDaemonConnector(conn, daemonServer).getMySQLSlaveStatus(
			chrootPath,
			osv,
			getPortForMySQLServer(conn, mysqlServer)
		);
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
		int mysqlDatabase,
		int mysqlSlave,
		CompressedDataOutputStream out
	) throws IOException, SQLException {
		BusinessHandler.checkPermission(conn, source, "getTableStatus", Permission.Name.get_mysql_table_status);
		// Check access
		checkAccessMySQLDatabase(conn, source, "getTableStatus", mysqlDatabase);
		int daemonServer;
		UnixPath chrootPath;
		int osv;
		int mysqlServer = getMySQLServerForMySQLDatabase(conn, mysqlDatabase);
		if(mysqlSlave==-1) {
			// Query the master
			daemonServer = getAOServerForMySQLServer(conn, mysqlServer);
			chrootPath = null;
			osv = ServerHandler.getOperatingSystemVersionForServer(conn, daemonServer);
			if(osv==-1) throw new SQLException("Unknown operating_system_version for aoServer: "+daemonServer);
		} else {
			// Query the slave
			int slaveMySQLServer = getMySQLServerForFailoverMySQLReplication(conn, mysqlSlave);
			if(slaveMySQLServer!=mysqlServer) throw new SQLException("slaveMySQLServer!=mysqlServer");
			if(conn.executeBooleanQuery("select ao_server is not null from backup.\"MysqlReplication\" where id=?", mysqlSlave)) {
				// ao_server-based
				daemonServer = conn.executeIntQuery("select ao_server from backup.\"MysqlReplication\" where id=?", mysqlSlave);
				chrootPath = null;
				osv = ServerHandler.getOperatingSystemVersionForServer(conn, daemonServer);
				if(osv==-1) throw new SQLException("Unknown operating_system_version for aoServer: "+daemonServer);
			} else {
				// replication-based
				daemonServer = conn.executeIntQuery("select bp.ao_server from backup.\"MysqlReplication\" fmr inner join backup.\"FileReplication\" ffr on fmr.replication=ffr.id inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where fmr.id=?", mysqlSlave);
				UnixPath toPath = conn.executeObjectQuery(
					ObjectFactories.unixPathFactory,
					"select bp.path from backup.\"MysqlReplication\" fmr inner join backup.\"FileReplication\" ffr on fmr.replication=ffr.id inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where fmr.id=?",
					mysqlSlave
				);
				int aoServer = getAOServerForMySQLServer(conn, slaveMySQLServer);
				osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
				if(osv==-1) throw new SQLException("Unknown operating_system_version for aoServer: "+aoServer);
				try {
					chrootPath = UnixPath.valueOf(toPath+"/"+ServerHandler.getHostnameForAOServer(conn, aoServer));
				} catch(ValidationException e) {
					throw new SQLException(e);
				}
			}
		}
		List<Database.TableStatus> tableStatuses = DaemonHandler.getDaemonConnector(conn, daemonServer).getMySQLTableStatus(
			chrootPath,
			osv,
			getPortForMySQLServer(conn, mysqlServer),
			getMySQLDatabaseName(conn, mysqlDatabase)
		);
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
		int mysqlDatabase,
		int mysqlSlave,
		List<MySQLTableName> tableNames,
		CompressedDataOutputStream out
	) throws IOException, SQLException {
		BusinessHandler.checkPermission(conn, source, "checkTables", Permission.Name.check_mysql_tables);
		// Check access
		checkAccessMySQLDatabase(conn, source, "checkTables", mysqlDatabase);
		int daemonServer;
		UnixPath chrootPath;
		int osv;
		int mysqlServer = getMySQLServerForMySQLDatabase(conn, mysqlDatabase);
		if(mysqlSlave==-1) {
			// Query the master
			daemonServer = getAOServerForMySQLServer(conn, mysqlServer);
			chrootPath = null;
			osv = ServerHandler.getOperatingSystemVersionForServer(conn, daemonServer);
			if(osv==-1) throw new SQLException("Unknown operating_system_version for aoServer: "+daemonServer);
		} else {
			// Query the slave
			int slaveMySQLServer = getMySQLServerForFailoverMySQLReplication(conn, mysqlSlave);
			if(slaveMySQLServer!=mysqlServer) throw new SQLException("slaveMySQLServer!=mysqlServer");
			if(conn.executeBooleanQuery("select ao_server is not null from backup.\"MysqlReplication\" where id=?", mysqlSlave)) {
				// ao_server-based
				daemonServer = conn.executeIntQuery("select ao_server from backup.\"MysqlReplication\" where id=?", mysqlSlave);
				chrootPath = null;
				osv = ServerHandler.getOperatingSystemVersionForServer(conn, daemonServer);
				if(osv==-1) throw new SQLException("Unknown operating_system_version for aoServer: "+daemonServer);
			} else {
				// replication-based
				daemonServer = conn.executeIntQuery("select bp.ao_server from backup.\"MysqlReplication\" fmr inner join backup.\"FileReplication\" ffr on fmr.replication=ffr.id inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where fmr.id=?", mysqlSlave);
				UnixPath toPath = conn.executeObjectQuery(
					ObjectFactories.unixPathFactory,
					"select bp.path from backup.\"MysqlReplication\" fmr inner join backup.\"FileReplication\" ffr on fmr.replication=ffr.id inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id where fmr.id=?",
					mysqlSlave
				);
				int aoServer = getAOServerForMySQLServer(conn, slaveMySQLServer);
				osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
				if(osv==-1) throw new SQLException("Unknown operating_system_version for aoServer: "+aoServer);
				try {
					chrootPath = UnixPath.valueOf(toPath+"/"+ServerHandler.getHostnameForAOServer(conn, aoServer));
				} catch(ValidationException e) {
					throw new SQLException(e);
				}
			}
		}
		List<Database.CheckTableResult> checkTableResults = DaemonHandler.getDaemonConnector(conn, daemonServer).checkMySQLTables(
			chrootPath,
			osv,
			getPortForMySQLServer(conn, mysqlServer),
			getMySQLDatabaseName(conn, mysqlDatabase),
			tableNames
		);
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
