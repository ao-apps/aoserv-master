/*
 * Copyright 2001-2013, 2015, 2017 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.AOServPermission;
import com.aoindustries.aoserv.client.AOServProtocol;
import com.aoindustries.aoserv.client.LinuxAccount;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.PasswordChecker;
import com.aoindustries.aoserv.client.PostgresUser;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.PostgresDatabaseName;
import com.aoindustries.aoserv.client.validator.PostgresServerName;
import com.aoindustries.aoserv.client.validator.PostgresUserId;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.util.IntList;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The <code>PostgresHandler</code> handles all the accesses to the PostgreSQL tables.
 *
 * @author  AO Industries, Inc.
 */
final public class PostgresHandler {

	private final static Map<Integer,Boolean> disabledPostgresServerUsers=new HashMap<>();
	private final static Map<PostgresUserId,Boolean> disabledPostgresUsers=new HashMap<>();

	public static void checkAccessPostgresDatabase(DatabaseConnection conn, RequestSource source, String action, int postgres_database) throws IOException, SQLException {
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
				ServerHandler.checkAccessServer(conn, source, action, getAOServerForPostgresDatabase(conn, postgres_database));
			}
		} else {
			checkAccessPostgresServerUser(conn, source, action, getDatDbaForPostgresDatabase(conn, postgres_database));
		}
	}

	public static void checkAccessPostgresServer(DatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, action, getAOServerForPostgresServer(conn, pkey));
	}

	public static void checkAccessPostgresServerUser(DatabaseConnection conn, RequestSource source, String action, int postgres_server_user) throws IOException, SQLException {
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
				ServerHandler.checkAccessServer(conn, source, action, getAOServerForPostgresServerUser(conn, postgres_server_user));
			}
		} else {
			checkAccessPostgresUser(conn, source, action, getUsernameForPostgresServerUser(conn, postgres_server_user));
		}
	}

	public static void checkAccessPostgresUser(DatabaseConnection conn, RequestSource source, String action, PostgresUserId username) throws IOException, SQLException {
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
				IntList psus = getPostgresServerUsersForPostgresUser(conn, username);
				boolean found = false;
				for(int psu : psus) {
					if(ServerHandler.canAccessServer(conn, source, getAOServerForPostgresServerUser(conn, psu))) {
						found=true;
						break;
					}
				}
				if(!found) {
					String message=
						"business_administrator.username="
						+source.getUsername()
						+" is not allowed to access postgres_user: action='"
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
	 * Adds a PostgreSQL database to the system.
	 */
	public static int addPostgresDatabase(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		PostgresDatabaseName name,
		int postgresServer,
		int datdba,
		int encoding,
		boolean enable_postgis
	) throws IOException, SQLException {
		// If requesting PostGIS, make sure the version of PostgreSQL supports it.
		if(
			enable_postgis
			&& conn.executeBooleanQuery("select pv.postgis_version is null from postgres_servers ps inner join postgres_versions pv on ps.version=pv.version where ps.pkey=?", postgresServer)
		) throw new SQLException("This version of PostgreSQL doesn't support PostGIS");

		// datdba must be on the same server and not be 'mail'
		int datdbaServer=getPostgresServerForPostgresServerUser(conn, datdba);
		if(datdbaServer!=postgresServer) throw new SQLException("(datdba.postgres_server="+datdbaServer+")!=(postgres_server="+postgresServer+")");
		PostgresUserId datdbaUsername=getUsernameForPostgresServerUser(conn, datdba);
		if(datdbaUsername.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add PostgresDatabase with datdba of '"+LinuxAccount.MAIL+'\'');
		if(isPostgresServerUserDisabled(conn, datdba)) throw new SQLException("Unable to add PostgresDatabase, PostgresServerUser disabled: "+datdba);
		// Look up the accounting code
		AccountingCode accounting=UsernameHandler.getBusinessForUsername(conn, datdbaUsername);
		// Encoding must exist for this version of the database
		if(
			!conn.executeBooleanQuery(
				"select\n"
				+ "  (\n"
				+ "    select\n"
				+ "      pe.pkey\n"
				+ "    from\n"
				+ "      postgres_servers ps,\n"
				+ "      postgres_encodings pe\n"
				+ "    where\n"
				+ "      ps.pkey=?\n"
				+ "      and ps.version=pe.postgres_version\n"
				+ "      and pe.pkey=?\n"
				+ "    limit 1\n"
				+ "  ) is not null",
				postgresServer,
				encoding
			)
		) throw new SQLException("PostgresServer #"+postgresServer+" does not support PostgresEncoding #"+encoding);

		// Must be allowed to access this server and package
		int aoServer=getAOServerForPostgresServer(conn, postgresServer);
		ServerHandler.checkAccessServer(conn, source, "addPostgresDatabase", aoServer);
		UsernameHandler.checkAccessUsername(conn, source, "addPostgresDatabase", datdbaUsername);
		// This sub-account must have access to the server
		BusinessHandler.checkBusinessAccessServer(conn, source, "addPostgresDatabase", accounting, aoServer);

		// Add the entry to the database
		int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('postgres_databases_pkey_seq')");
		conn.executeUpdate(
			"insert into\n"
			+ "  postgres_databases\n"
			+ "values(\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  false,\n"
			+ "  true,\n"
			+ "  ?\n"
			+ ")",
			pkey,
			name,
			postgresServer,
			datdba,
			encoding,
			enable_postgis
		);

		// Notify all clients of the update, the server will detect this change and automatically add the database
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.POSTGRES_DATABASES,
			accounting,
			aoServer,
			false
		);
		return pkey;
	}

	/**
	 * Adds a PostgreSQL server user.
	 */
	public static int addPostgresServerUser(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		PostgresUserId username, 
		int postgresServer
	) throws IOException, SQLException {
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add PostgresServerUser for user '"+LinuxAccount.MAIL+'\'');

		checkAccessPostgresUser(conn, source, "addPostgresServerUser", username);
		if(isPostgresUserDisabled(conn, username)) throw new SQLException("Unable to add PostgresServerUser, PostgresUser disabled: "+username);
		int aoServer=getAOServerForPostgresServer(conn, postgresServer);
		ServerHandler.checkAccessServer(conn, source, "addPostgresServerUser", aoServer);
		// This sub-account must have access to the server
		UsernameHandler.checkUsernameAccessServer(conn, source, "addPostgresServerUser", username, aoServer);

		int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('postgres_server_users_pkey_seq')");

		conn.executeUpdate(
			"insert into postgres_server_users values(?,?,?,null,null)",
			pkey,
			username,
			postgresServer
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.POSTGRES_SERVER_USERS,
			UsernameHandler.getBusinessForUsername(conn, username),
			aoServer,
			true
		);
		return pkey;
	}

	/**
	 * Adds a PostgreSQL user.
	 */
	public static void addPostgresUser(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		PostgresUserId username
	) throws IOException, SQLException {
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add PostgresUser for user '"+LinuxAccount.MAIL+'\'');
		UsernameHandler.checkAccessUsername(conn, source, "addPostgresUser", username);
		if(UsernameHandler.isUsernameDisabled(conn, username)) throw new SQLException("Unable to add PostgresUser, Username disabled: "+username);

		conn.executeUpdate(
			"insert into postgres_users(username) values(?)",
			username
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.POSTGRES_USERS,
			UsernameHandler.getBusinessForUsername(conn, username),
			InvalidateList.allServers,
			false
		);
	}

	public static void disablePostgresServerUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		int pkey
	) throws IOException, SQLException {
		if(isPostgresServerUserDisabled(conn, pkey)) throw new SQLException("PostgresServerUser is already disabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "disablePostgresServerUser", disableLog, false);
		checkAccessPostgresServerUser(conn, source, "disablePostgresServerUser", pkey);

		conn.executeUpdate(
			"update postgres_server_users set disable_log=? where pkey=?",
			disableLog,
			pkey
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.POSTGRES_SERVER_USERS,
			getBusinessForPostgresServerUser(conn, pkey),
			getAOServerForPostgresServerUser(conn, pkey),
			false
		);
	}

	public static void disablePostgresUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		PostgresUserId username
	) throws IOException, SQLException {
		if(isPostgresUserDisabled(conn, username)) throw new SQLException("PostgresUser is already disabled: "+username);
		BusinessHandler.checkAccessDisableLog(conn, source, "disablePostgresUser", disableLog, false);
		checkAccessPostgresUser(conn, source, "disablePostgresUser", username);
		IntList psus=getPostgresServerUsersForPostgresUser(conn, username);
		for(int c=0;c<psus.size();c++) {
			int psu=psus.getInt(c);
			if(!isPostgresServerUserDisabled(conn, psu)) {
				throw new SQLException("Cannot disable PostgresUser '"+username+"': PostgresServerUser not disabled: "+psu);
			}
		}

		conn.executeUpdate(
			"update postgres_users set disable_log=? where username=?",
			disableLog,
			username
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.POSTGRES_USERS,
			UsernameHandler.getBusinessForUsername(conn, username),
			UsernameHandler.getServersForUsername(conn, username),
			false
		);
	}

	/**
	 * Dumps a PostgreSQL database
	 */
	public static void dumpPostgresDatabase(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataOutputStream out,
		int dbPKey,
		boolean gzip
	) throws IOException, SQLException {
		checkAccessPostgresDatabase(conn, source, "dumpPostgresDatabase", dbPKey);

		int aoServer=getAOServerForPostgresDatabase(conn, dbPKey);
		DaemonHandler.getDaemonConnector(conn, aoServer).dumpPostgresDatabase(
			dbPKey,
			gzip,
			(long dumpSize) -> {
				if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_80_0) >= 0) {
					out.writeLong(dumpSize);
				}
			},
			out
		);
	}

	public static void enablePostgresServerUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int disableLog=getDisableLogForPostgresServerUser(conn, pkey);
		if(disableLog==-1) throw new SQLException("PostgresServerUser is already enabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "enablePostgresServerUser", disableLog, true);
		checkAccessPostgresServerUser(conn, source, "enablePostgresServerUser", pkey);
		PostgresUserId pu=getUsernameForPostgresServerUser(conn, pkey);
		if(isPostgresUserDisabled(conn, pu)) throw new SQLException("Unable to enable PostgresServerUser #"+pkey+", PostgresUser not enabled: "+pu);

		conn.executeUpdate(
			"update postgres_server_users set disable_log=null where pkey=?",
			pkey
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.POSTGRES_SERVER_USERS,
			UsernameHandler.getBusinessForUsername(conn, pu),
			getAOServerForPostgresServerUser(conn, pkey),
			false
		);
	}

	public static void enablePostgresUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		PostgresUserId username
	) throws IOException, SQLException {
		int disableLog=getDisableLogForPostgresUser(conn, username);
		if(disableLog==-1) throw new SQLException("PostgresUser is already enabled: "+username);
		BusinessHandler.checkAccessDisableLog(conn, source, "enablePostgresUser", disableLog, true);
		UsernameHandler.checkAccessUsername(conn, source, "enablePostgresUser", username);
		if(UsernameHandler.isUsernameDisabled(conn, username)) throw new SQLException("Unable to enable PostgresUser '"+username+"', Username not enabled: "+username);

		conn.executeUpdate(
			"update postgres_users set disable_log=null where username=?",
			username
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.POSTGRES_USERS,
			UsernameHandler.getBusinessForUsername(conn, username),
			UsernameHandler.getServersForUsername(conn, username),
			false
		);
	}

	/**
	 * Generates a unique PostgreSQL database name.
	 */
	public static PostgresDatabaseName generatePostgresDatabaseName(
		DatabaseConnection conn,
		String template_base,
		String template_added
	) throws IOException, SQLException {
		// Load the entire list of postgres database names
		Set<PostgresDatabaseName> names = conn.executeObjectCollectionQuery(
			new HashSet<>(),
			ObjectFactories.postgresDatabaseNameFactory,
			"select name from postgres_databases group by name"
		);
		// Find one that is not used
		for(int c=0;c<Integer.MAX_VALUE;c++) {
			PostgresDatabaseName name;
			try {
				name = PostgresDatabaseName.valueOf((c==0) ? template_base : (template_base+template_added+c));
			} catch(ValidationException e) {
				throw new SQLException(e.getLocalizedMessage(), e);
			}
			if(!names.contains(name)) return name;
		}
		// If could not find one, report and error
		throw new SQLException("Unable to find available PostgreSQL database name for template_base="+template_base+" and template_added="+template_added);
	}

	public static int getDisableLogForPostgresServerUser(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from postgres_server_users where pkey=?", pkey);
	}

	public static int getDisableLogForPostgresUser(DatabaseConnection conn, PostgresUserId username) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from postgres_users where username=?", username);
	}

	public static IntList getPostgresServerUsersForPostgresUser(DatabaseConnection conn, PostgresUserId username) throws IOException, SQLException {
		return conn.executeIntListQuery("select pkey from postgres_server_users where username=?", username);
	}

	public static PostgresUserId getUsernameForPostgresServerUser(DatabaseConnection conn, int psu) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.postgresUserIdFactory,
			"select username from postgres_server_users where pkey=?",
			psu
		);
	}

	public static void invalidateTable(SchemaTable.TableID tableID) {
		switch(tableID) {
			case POSTGRES_SERVER_USERS :
				synchronized(PostgresHandler.class) {
					disabledPostgresServerUsers.clear();
				}
				break;
			case POSTGRES_USERS :
				synchronized(PostgresHandler.class) {
					disabledPostgresUsers.clear();
				}
				break;
		}
	}

	public static boolean isPostgresServerUserDisabled(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		synchronized(PostgresHandler.class) {
			Integer I = pkey;
			Boolean O=disabledPostgresServerUsers.get(I);
			if(O!=null) return O;
			boolean isDisabled=getDisableLogForPostgresServerUser(conn, pkey)!=-1;
			disabledPostgresServerUsers.put(I, isDisabled);
			return isDisabled;
		}
	}

	public static boolean isPostgresUser(DatabaseConnection conn, UserId username) throws IOException, SQLException {
		return conn.executeBooleanQuery(
			"select\n"
			+ "  (\n"
			+ "    select\n"
			+ "      username\n"
			+ "    from\n"
			+ "      postgres_users\n"
			+ "    where\n"
			+ "      username=?\n"
			+ "    limit 1\n"
			+ "  ) is not null",
			username
		);
	}

	public static boolean isPostgresUserDisabled(DatabaseConnection conn, PostgresUserId username) throws IOException, SQLException {
		synchronized(PostgresHandler.class) {
			Boolean O=disabledPostgresUsers.get(username);
			if(O!=null) return O;
			boolean isDisabled=getDisableLogForPostgresUser(conn, username)!=-1;
			disabledPostgresUsers.put(username, isDisabled);
			return isDisabled;
		}
	}

	/**
	 * Determines if a PostgreSQL database name is available.
	 */
	public static boolean isPostgresDatabaseNameAvailable(
		DatabaseConnection conn,
		RequestSource source,
		PostgresDatabaseName name,
		int postgresServer
	) throws IOException, SQLException {
		int aoServer=getAOServerForPostgresServer(conn, postgresServer);
		ServerHandler.checkAccessServer(
			conn,
			source,
			"isPostgresDatabaseNameAvailable",
			aoServer
		);
		return conn.executeBooleanQuery(
			"select\n"
			+ "  (\n"
			+ "    select\n"
			+ "      pkey\n"
			+ "    from\n"
			+ "      postgres_databases\n"
			+ "    where\n"
			+ "      name=?\n"
			+ "      and postgres_server=?\n"
			+ "    limit 1\n"
			+ "  ) is null",
			name,
			postgresServer
		);
	}

	/**
	 * Determines if a PostgreSQL server name is available.
	 */
	public static boolean isPostgresServerNameAvailable(
		DatabaseConnection conn,
		RequestSource source,
		PostgresServerName name,
		int aoServer
	) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "isPostgresServerNameAvailable", aoServer);
		return conn.executeBooleanQuery("select (select pkey from postgres_servers where name=? and ao_server=? limit 1) is null", name, aoServer);
	}

	public static boolean isPostgresServerUserPasswordSet(
		DatabaseConnection conn,
		RequestSource source, 
		int psu
	) throws IOException, SQLException {
		checkAccessPostgresServerUser(conn, source, "isPostgresServerUserPasswordSet", psu);
		if(isPostgresServerUserDisabled(conn, psu)) throw new SQLException("Unable to determine if PostgresServerUser password is set, account disabled: "+psu);
		PostgresUserId username=getUsernameForPostgresServerUser(conn, psu);

		int aoServer=getAOServerForPostgresServerUser(conn, psu);
		String password=DaemonHandler.getDaemonConnector(conn, aoServer).getPostgresUserPassword(psu);
		return !PostgresUser.NO_PASSWORD_DB_VALUE.equals(password);
	}

	/**
	 * Removes a PostgresDatabase from the system.
	 */
	public static void removePostgresDatabase(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		checkAccessPostgresDatabase(conn, source, "removePostgresDatabase", pkey);

		removePostgresDatabase(conn, invalidateList, pkey);
	}

	/**
	 * Removes a PostgresDatabase from the system.
	 */
	public static void removePostgresDatabase(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		// Remove the database entry
		AccountingCode accounting = getBusinessForPostgresDatabase(conn, pkey);
		int aoServer=getAOServerForPostgresDatabase(conn, pkey);
		conn.executeUpdate("delete from postgres_databases where pkey=?", pkey);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.POSTGRES_DATABASES,
			accounting,
			aoServer,
			false
		);
	}

	/**
	 * Removes a PostgresServerUser from the system.
	 */
	public static void removePostgresServerUser(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		checkAccessPostgresServerUser(conn, source, "removePostgresServerUser", pkey);

		PostgresUserId username=getUsernameForPostgresServerUser(conn, pkey);
		if(username.equals(PostgresUser.POSTGRES)) throw new SQLException("Not allowed to remove PostgresUser for user '"+PostgresUser.POSTGRES+'\'');

		// Get the details for later use
		int aoServer=getAOServerForPostgresServerUser(conn, pkey);
		AccountingCode accounting = getBusinessForPostgresServerUser(conn, pkey);

		// Make sure that this is not the DBA for any databases
		int count=conn.executeIntQuery("select count(*) from postgres_databases where datdba=?", pkey);
		if(count>0) throw new SQLException("PostgresServerUser #"+pkey+" cannot be removed because it is the datdba for "+count+(count==1?" database":" databases"));

		// Remove the postgres_server_user
		conn.executeUpdate("delete from postgres_server_users where pkey=?", pkey);

		// Notify all clients of the updates
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.POSTGRES_SERVER_USERS,
			accounting,
			aoServer,
			true
		);
	}

	/**
	 * Removes a PostgresUser from the system.
	 */
	public static void removePostgresUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		PostgresUserId username
	) throws IOException, SQLException {
		checkAccessPostgresUser(conn, source, "removePostgresUser", username);

		removePostgresUser(conn, invalidateList, username);
	}

	/**
	 * Removes a PostgresUser from the system.
	 */
	public static void removePostgresUser(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		PostgresUserId username
	) throws IOException, SQLException {
		if(username.equals(PostgresUser.POSTGRES)) throw new SQLException("Not allowed to remove PostgresUser named '"+PostgresUser.POSTGRES+'\'');
		AccountingCode accounting = UsernameHandler.getBusinessForUsername(conn, username);

		// Remove the postgres_server_user
		IntList aoServers=conn.executeIntListQuery("select ps.ao_server from postgres_server_users psu, postgres_servers ps where psu.username=? and psu.postgres_server=ps.pkey", username);
		if(aoServers.size()>0) {
			conn.executeUpdate("delete from postgres_server_users where username=?", username);
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.POSTGRES_SERVER_USERS,
				accounting,
				aoServers,
				false
			);
		}

		// Remove the postgres_user
		conn.executeUpdate("delete from postgres_users where username=?", username);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.POSTGRES_USERS,
			accounting,
			BusinessHandler.getServersForBusiness(conn, accounting),
			false
		);
	}

	/**
	 * Sets a PostgreSQL password.
	 */
	public static void setPostgresServerUserPassword(
		DatabaseConnection conn,
		RequestSource source,
		int postgres_server_user,
		String password
	) throws IOException, SQLException {
		BusinessHandler.checkPermission(conn, source, "setPostgresServerUserPassword", AOServPermission.Permission.set_postgres_server_user_password);
		checkAccessPostgresServerUser(conn, source, "setPostgresServerUserPassword", postgres_server_user);
		if(isPostgresServerUserDisabled(conn, postgres_server_user)) throw new SQLException("Unable to set PostgresServerUser password, account disabled: "+postgres_server_user);

		// Get the server for the user
		int aoServer=getAOServerForPostgresServerUser(conn, postgres_server_user);
		PostgresUserId username=getUsernameForPostgresServerUser(conn, postgres_server_user);

		// No setting the super user password
		if(username.equals(PostgresUser.POSTGRES)) throw new SQLException("The PostgreSQL "+PostgresUser.POSTGRES+" password may not be set.");

		// Perform the password check here, too.
		if(password!=PostgresUser.NO_PASSWORD) {
			List<PasswordChecker.Result> results = PostgresUser.checkPassword(username, password);
			if(PasswordChecker.hasResults(results)) throw new SQLException("Invalid password: "+PasswordChecker.getResultsString(results).replace('\n', '|'));
		}

		// Contact the daemon for the update
		DaemonHandler.getDaemonConnector(conn, aoServer).setPostgresUserPassword(postgres_server_user, password);
	}

	public static void setPostgresServerUserPredisablePassword(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int psu,
		String password
	) throws IOException, SQLException {
		checkAccessPostgresServerUser(conn, source, "setPostgresServerUserPredisablePassword", psu);
		if(password==null) {
			if(isPostgresServerUserDisabled(conn, psu)) throw new SQLException("Unable to clear PostgresServerUser predisable password, account disabled: "+psu);
		} else {
			if(!isPostgresServerUserDisabled(conn, psu)) throw new SQLException("Unable to set PostgresServerUser predisable password, account not disabled: "+psu);
		}

		// Update the database
		conn.executeUpdate(
			"update postgres_server_users set predisable_password=? where pkey=?",
			password,
			psu
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.POSTGRES_SERVER_USERS,
			getBusinessForPostgresServerUser(conn, psu),
			getAOServerForPostgresServerUser(conn, psu),
			false
		);
	}

	public static void waitForPostgresDatabaseRebuild(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "waitForPostgresDatabaseRebuild", aoServer);
		ServerHandler.waitForInvalidates(aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).waitForPostgresDatabaseRebuild();
	}

	public static void waitForPostgresServerRebuild(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "waitForPostgresServerRebuild", aoServer);
		ServerHandler.waitForInvalidates(aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).waitForPostgresServerRebuild();
	}

	public static void waitForPostgresUserRebuild(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "waitForPostgresUserRebuild", aoServer);
		ServerHandler.waitForInvalidates(aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).waitForPostgresUserRebuild();
	}

	public static AccountingCode getBusinessForPostgresDatabase(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  postgres_databases pd,\n"
			+ "  postgres_server_users psu,\n"
			+ "  usernames un,\n"
			+ "  packages pk\n"
			+ "where\n"
			+ "  pd.pkey=?\n"
			+ "  and pd.datdba=psu.pkey\n"
			+ "  and psu.username=un.username\n"
			+ "  and un.package=pk.name",
			pkey
		);
	}

	public static int getPackageForPostgresDatabase(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  pk.pkey\n"
			+ "from\n"
			+ "  postgres_databases pd,\n"
			+ "  postgres_server_users psu,\n"
			+ "  usernames un,\n"
			+ "  packages pk\n"
			+ "where\n"
			+ "  pd.pkey=?\n"
			+ "  and pd.datdba=psu.pkey\n"
			+ "  and psu.username=un.username\n"
			+ "  and un.package=pk.name",
			pkey
		);
	}

	public static AccountingCode getBusinessForPostgresServerUser(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select pk.accounting from postgres_server_users psu, usernames un, packages pk where psu.username=un.username and un.package=pk.name and psu.pkey=?",
			pkey
		);
	}

	public static int getAOServerForPostgresServer(DatabaseConnection conn, int postgresServer) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from postgres_servers where pkey=?", postgresServer);
	}

	public static int getPortForPostgresServer(DatabaseConnection conn, int postgresServer) throws IOException, SQLException {
		return conn.executeIntQuery("select nb.port from postgres_servers ps, net_binds nb where ps.pkey=? and ps.net_bind=nb.pkey", postgresServer);
	}

	public static String getMinorVersionForPostgresServer(DatabaseConnection conn, int postgresServer) throws IOException, SQLException {
		return conn.executeStringQuery(
			"select\n"
			+ "  pv.minor_version\n"
			+ "from\n"
			+ "  postgres_servers ps,\n"
			+ "  postgres_versions pv\n"
			+ "where\n"
			+ "  ps.pkey=?\n"
			+ "  and ps.version=pv.version",
			postgresServer
		);
	}

	public static int getPostgresServerForPostgresDatabase(DatabaseConnection conn, int postgresDatabase) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select postgres_server from postgres_databases where pkey=?",
			postgresDatabase
		);
	}

	public static int getPostgresServerForPostgresServerUser(DatabaseConnection conn, int postgres_server_user) throws IOException, SQLException {
		return conn.executeIntQuery("select postgres_server from postgres_server_users where pkey=?", postgres_server_user);
	}

	public static int getAOServerForPostgresDatabase(DatabaseConnection conn, int postgresDatabase) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  ps.ao_server\n"
			+ "from\n"
			+ "  postgres_databases pd,\n"
			+ "  postgres_servers ps\n"
			+ "where\n"
			+ "  pd.pkey=?\n"
			+ "  and pd.postgres_server=ps.pkey",
			postgresDatabase
		);
	}

	public static int getDatDbaForPostgresDatabase(DatabaseConnection conn, int postgresDatabase) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  datdba\n"
			+ "from\n"
			+ "  postgres_databases\n"
			+ "where\n"
			+ "  pkey=?",
			postgresDatabase
		);
	}

	public static int getAOServerForPostgresServerUser(DatabaseConnection conn, int postgres_server_user) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  ps.ao_server\n"
			+ "from\n"
			+ "  postgres_server_users psu,\n"
			+ "  postgres_servers ps\n"
			+ "where\n"
			+ "  psu.pkey=?\n"
			+ "  and psu.postgres_server=ps.pkey",
			postgres_server_user
		);
	}

	public static void restartPostgreSQL(
		DatabaseConnection conn,
		RequestSource source,
		int postgresServer
	) throws IOException, SQLException {
		int aoServer=getAOServerForPostgresServer(conn, postgresServer);
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_postgresql");
		if(!canControl) throw new SQLException("Not allowed to restart PostgreSQL on "+aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).restartPostgres(postgresServer);
	}

	public static void startPostgreSQL(
		DatabaseConnection conn,
		RequestSource source,
		int postgresServer
	) throws IOException, SQLException {
		int aoServer=getAOServerForPostgresServer(conn, postgresServer);
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_postgresql");
		if(!canControl) throw new SQLException("Not allowed to start PostgreSQL on "+aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).startPostgreSQL(postgresServer);
	}

	public static void stopPostgreSQL(
		DatabaseConnection conn,
		RequestSource source,
		int postgresServer
	) throws IOException, SQLException {
		int aoServer=getAOServerForPostgresServer(conn, postgresServer);
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_postgresql");
		if(!canControl) throw new SQLException("Not allowed to stop PostgreSQL on "+aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).stopPostgreSQL(postgresServer);
	}

	private PostgresHandler() {}
}
