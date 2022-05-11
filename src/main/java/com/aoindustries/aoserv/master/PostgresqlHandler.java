/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2017, 2018, 2019, 2020, 2021, 2022  AO Industries, Inc.
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

import com.aoapps.collections.AoCollections;
import com.aoapps.collections.IntList;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoapps.lang.validation.ValidationException;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.password.PasswordChecker;
import com.aoindustries.aoserv.client.postgresql.Database;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.daemon.client.AoservDaemonConnector;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The <code>PostgresHandler</code> handles all the accesses to the PostgreSQL tables.
 *
 * @author  AO Industries, Inc.
 */
public final class PostgresqlHandler {

  /** Make no instances. */
  private PostgresqlHandler() {
    throw new AssertionError();
  }

  private static final Map<Integer, Boolean> disabledUserServers = new HashMap<>();
  private static final Map<com.aoindustries.aoserv.client.postgresql.User.Name, Boolean> disabledUsers = new HashMap<>();

  public static void checkAccessDatabase(DatabaseConnection conn, RequestSource source, String action, int database) throws IOException, SQLException {
    User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu != null) {
      if (AoservMaster.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
        NetHostHandler.checkAccessHost(conn, source, action, getLinuxServerForDatabase(conn, database));
      }
    } else {
      checkAccessUserServer(conn, source, action, getDatdbaForDatabase(conn, database));
    }
  }

  public static void checkAccessServer(DatabaseConnection conn, RequestSource source, String action, int postgresqlServer) throws IOException, SQLException {
    NetHostHandler.checkAccessHost(conn, source, action, getLinuxServerForServer(conn, postgresqlServer));
  }

  public static void checkAccessUserServer(DatabaseConnection conn, RequestSource source, String action, int userServer) throws IOException, SQLException {
    User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu != null) {
      if (AoservMaster.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
        NetHostHandler.checkAccessHost(conn, source, action, getLinuxServerForUserServer(conn, userServer));
      }
    } else {
      checkAccessUser(conn, source, action, getUserForUserServer(conn, userServer));
    }
  }

  public static void checkAccessUser(DatabaseConnection conn, RequestSource source, String action, com.aoindustries.aoserv.client.postgresql.User.Name user) throws IOException, SQLException {
    User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu != null) {
      if (AoservMaster.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
        IntList psus = getUserServersForUser(conn, user);
        boolean found = false;
        for (int psu : psus) {
          if (NetHostHandler.canAccessHost(conn, source, getLinuxServerForUserServer(conn, psu))) {
            found = true;
            break;
          }
        }
        if (!found) {
          String message =
              "currentAdministrator="
                  + source.getCurrentAdministrator()
                  + " is not allowed to access postgres_user: action='"
                  + action
                  + ", user="
                  + user;
          throw new SQLException(message);
        }
      }
    } else {
      AccountUserHandler.checkAccessUser(conn, source, action, user);
    }
  }

  /**
   * Adds a PostgreSQL database to the system.
   */
  public static int addDatabase(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      Database.Name name,
      int postgresqlServer,
      int datdba,
      int encoding,
      boolean enablePostgis
  ) throws IOException, SQLException {
    if (Database.isSpecial(name)) {
      throw new SQLException("Refusing to add special PostgreSQL database: " + name);
    }

    // If requesting PostGIS, make sure the version of PostgreSQL supports it.
    if (
        enablePostgis
            && conn.queryBoolean("select pv.postgis_version is null from postgresql.\"Server\" ps inner join postgresql.\"Version\" pv on ps.version = pv.version where ps.bind = ?", postgresqlServer)
    ) {
      throw new SQLException("This version of PostgreSQL doesn't support PostGIS");
    }

    // datdba must be on the same server and not be 'mail'
    int datdbaServer = getServerForUserServer(conn, datdba);
    if (datdbaServer != postgresqlServer) {
      throw new SQLException("(datdba.postgres_server=" + datdbaServer + ") != (postgres_server=" + postgresqlServer + ")");
    }
    final com.aoindustries.aoserv.client.postgresql.User.Name datdbaUsername = getUserForUserServer(conn, datdba);
    if (datdbaUsername.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) {
      throw new SQLException("Not allowed to add Database with datdba of '" + com.aoindustries.aoserv.client.linux.User.MAIL + '\'');
    }
    if (isUserServerDisabled(conn, datdba)) {
      throw new SQLException("Unable to add Database, UserServer disabled: " + datdba);
    }
    // Look up the accounting code
    final Account.Name account = AccountUserHandler.getAccountForUser(conn, datdbaUsername);
    // Encoding must exist for this version of the database
    if (
        !conn.queryBoolean(
            "SELECT EXISTS (\n"
                + "  SELECT\n"
                + "    pe.id\n"
                + "  FROM\n"
                + "    postgresql.\"Server\" ps\n"
                + "    INNER JOIN postgresql.\"Encoding\" pe ON ps.version = pe.postgres_version\n"
                + "  WHERE\n"
                + "    ps.bind = ?\n"
                + "    AND pe.id = ?\n"
                + ")",
            postgresqlServer,
            encoding
        )
    ) {
      throw new SQLException("Server #" + postgresqlServer + " does not support Encoding #" + encoding);
    }

    // Must be allowed to access this server and package
    int linuxServer = getLinuxServerForServer(conn, postgresqlServer);
    NetHostHandler.checkAccessHost(conn, source, "addDatabase", linuxServer);
    AccountUserHandler.checkAccessUser(conn, source, "addDatabase", datdbaUsername);
    // This sub-account must have access to the server
    AccountHandler.checkAccountAccessHost(conn, source, "addDatabase", account, linuxServer);

    // Add the entry to the database
    int database = conn.updateInt(
        "INSERT INTO\n"
            + "  postgresql.\"Database\"\n"
            + "VALUES (\n"
            + "  default,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  false,\n"
            + "  true,\n"
            + "  ?\n"
            + ") RETURNING id",
        name,
        postgresqlServer,
        datdba,
        encoding,
        enablePostgis
    );

    // Notify all clients of the update, the server will detect this change and automatically add the database
    invalidateList.addTable(
        conn,
        Table.TableId.POSTGRES_DATABASES,
        account,
        linuxServer,
        false
    );
    return database;
  }

  /**
   * Adds a PostgreSQL server user.
   */
  public static int addUserServer(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      com.aoindustries.aoserv.client.postgresql.User.Name user,
      int postgresqlServer
  ) throws IOException, SQLException {
    if (com.aoindustries.aoserv.client.postgresql.User.isSpecial(user)) {
      throw new SQLException("Refusing to add special PostgreSQL user: " + user);
    }
    if (user.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) {
      throw new SQLException("Not allowed to add UserServer for user '" + com.aoindustries.aoserv.client.linux.User.MAIL + '\'');
    }

    checkAccessUser(conn, source, "addUserServer", user);
    if (isUserDisabled(conn, user)) {
      throw new SQLException("Unable to add UserServer, User disabled: " + user);
    }
    int linuxServer = getLinuxServerForServer(conn, postgresqlServer);
    NetHostHandler.checkAccessHost(conn, source, "addUserServer", linuxServer);
    // This sub-account must have access to the server
    AccountUserHandler.checkUserAccessHost(conn, source, "addUserServer", user, linuxServer);

    int userServer = conn.updateInt(
        "INSERT INTO postgresql.\"UserServer\" VALUES (default,?,?,null,null) RETURNING id",
        user,
        postgresqlServer
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.POSTGRES_SERVER_USERS,
        AccountUserHandler.getAccountForUser(conn, user),
        linuxServer,
        true
    );
    return userServer;
  }

  /**
   * Adds a PostgreSQL user.
   */
  public static void addUser(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      com.aoindustries.aoserv.client.postgresql.User.Name user
  ) throws IOException, SQLException {
    if (com.aoindustries.aoserv.client.postgresql.User.isSpecial(user)) {
      throw new SQLException("Refusing to add special PostgreSQL user: " + user);
    }
    if (user.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) {
      throw new SQLException("Not allowed to add User for user '" + com.aoindustries.aoserv.client.linux.User.MAIL + '\'');
    }

    AccountUserHandler.checkAccessUser(conn, source, "addUser", user);
    if (AccountUserHandler.isUserDisabled(conn, user)) {
      throw new SQLException("Unable to add User, Username disabled: " + user);
    }

    conn.update(
        "insert into postgresql.\"User\"(username) values(?)",
        user
    );

    // Notify all clients of the update
    invalidateList.addTable(conn,
        Table.TableId.POSTGRES_USERS,
        AccountUserHandler.getAccountForUser(conn, user),
        InvalidateList.allHosts,
        false
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

    com.aoindustries.aoserv.client.postgresql.User.Name pu = getUserForUserServer(conn, userServer);
    if (com.aoindustries.aoserv.client.postgresql.User.isSpecial(pu)) {
      throw new SQLException("Refusing to disable special PostgreSQL user: " + pu);
    }
    if (isUserServerDisabled(conn, userServer)) {
      throw new SQLException("UserServer is already disabled: " + userServer);
    }

    conn.update(
        "update postgresql.\"UserServer\" set disable_log=? where id=?",
        disableLog,
        userServer
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.POSTGRES_SERVER_USERS,
        getAccountForUserServer(conn, userServer),
        getLinuxServerForUserServer(conn, userServer),
        false
    );
  }

  public static void disableUser(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int disableLog,
      com.aoindustries.aoserv.client.postgresql.User.Name user
  ) throws IOException, SQLException {
    if (com.aoindustries.aoserv.client.postgresql.User.isSpecial(user)) {
      throw new SQLException("Refusing to disable special PostgreSQL user: " + user);
    }

    AccountHandler.checkAccessDisableLog(conn, source, "disableUser", disableLog, false);
    checkAccessUser(conn, source, "disableUser", user);

    if (isUserDisabled(conn, user)) {
      throw new SQLException("User is already disabled: " + user);
    }

    IntList userServers = getUserServersForUser(conn, user);
    for (int c = 0; c < userServers.size(); c++) {
      int userServer = userServers.getInt(c);
      if (!isUserServerDisabled(conn, userServer)) {
        throw new SQLException("Cannot disable User '" + user + "': UserServer not disabled: " + userServer);
      }
    }

    conn.update(
        "update postgresql.\"User\" set disable_log=? where username=?",
        disableLog,
        user
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.POSTGRES_USERS,
        AccountUserHandler.getAccountForUser(conn, user),
        AccountUserHandler.getHostsForUser(conn, user),
        false
    );
  }

  /**
   * Dumps a PostgreSQL database.
   */
  public static void dumpDatabase(
      DatabaseConnection conn,
      RequestSource source,
      StreamableOutput out,
      int database,
      boolean gzip
  ) throws IOException, SQLException {
    checkAccessDatabase(conn, source, "dumpDatabase", database);

    int linuxServer = getLinuxServerForDatabase(conn, database);
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.dumpPostgresDatabase(
        database,
        gzip,
        dumpSize -> {
          if (source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_0) >= 0) {
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
    if (disableLog == -1) {
      throw new SQLException("UserServer is already enabled: " + userServer);
    }
    AccountHandler.checkAccessDisableLog(conn, source, "enableUserServer", disableLog, true);

    com.aoindustries.aoserv.client.postgresql.User.Name pu = getUserForUserServer(conn, userServer);
    if (com.aoindustries.aoserv.client.postgresql.User.isSpecial(pu)) {
      throw new SQLException("Refusing to enable special PostgreSQL user: " + pu);
    }
    if (isUserDisabled(conn, pu)) {
      throw new SQLException("Unable to enable UserServer #" + userServer + ", User not enabled: " + pu);
    }

    conn.update(
        "update postgresql.\"UserServer\" set disable_log=null where id=?",
        userServer
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.POSTGRES_SERVER_USERS,
        AccountUserHandler.getAccountForUser(conn, pu),
        getLinuxServerForUserServer(conn, userServer),
        false
    );
  }

  public static void enableUser(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      com.aoindustries.aoserv.client.postgresql.User.Name user
  ) throws IOException, SQLException {
    if (com.aoindustries.aoserv.client.postgresql.User.isSpecial(user)) {
      throw new SQLException("Refusing to enable special PostgreSQL user: " + user);
    }

    AccountUserHandler.checkAccessUser(conn, source, "enableUser", user);
    int disableLog = getDisableLogForUser(conn, user);
    if (disableLog == -1) {
      throw new SQLException("User is already enabled: " + user);
    }
    AccountHandler.checkAccessDisableLog(conn, source, "enableUser", disableLog, true);

    if (AccountUserHandler.isUserDisabled(conn, user)) {
      throw new SQLException("Unable to enable User '" + user + "', Username not enabled: " + user);
    }

    conn.update(
        "update postgresql.\"User\" set disable_log=null where username=?",
        user
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.POSTGRES_USERS,
        AccountUserHandler.getAccountForUser(conn, user),
        AccountUserHandler.getHostsForUser(conn, user),
        false
    );
  }

  /**
   * Generates a unique PostgreSQL database name.
   */
  public static Database.Name generateDatabaseName(
      DatabaseConnection conn,
      String templateBase,
      String templateAdded
  ) throws IOException, SQLException {
    // Load the entire list of postgres database names
    Set<Database.Name> names = conn.queryNewCollection(
        AoCollections::newHashSet,
        ObjectFactories.postgresqlDatabaseNameFactory,
        "select name from postgresql.\"Database\" group by name"
    );
    // Find one that is not used
    for (int c = 0; c < Integer.MAX_VALUE; c++) {
      Database.Name name;
      try {
        name = Database.Name.valueOf((c == 0) ? templateBase : (templateBase + templateAdded + c));
      } catch (ValidationException e) {
        throw new SQLException(e.getLocalizedMessage(), e);
      }
      if (!names.contains(name)) {
        return name;
      }
    }
    // If could not find one, report and error
    throw new SQLException("Unable to find available PostgreSQL database name for template_base=" + templateBase + " and template_added=" + templateAdded);
  }

  public static int getDisableLogForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
    return conn.queryInt("select coalesce(disable_log, -1) from postgresql.\"UserServer\" where id=?", userServer);
  }

  public static int getDisableLogForUser(DatabaseConnection conn, com.aoindustries.aoserv.client.postgresql.User.Name user) throws IOException, SQLException {
    return conn.queryInt("select coalesce(disable_log, -1) from postgresql.\"User\" where username=?", user);
  }

  public static Database.Name getNameForDatabase(DatabaseConnection conn, int database) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.postgresqlDatabaseNameFactory,
        "select \"name\" from postgresql.\"Database\" where id=?",
        database
    );
  }

  public static IntList getUserServersForUser(DatabaseConnection conn, com.aoindustries.aoserv.client.postgresql.User.Name user) throws IOException, SQLException {
    return conn.queryIntList("select id from postgresql.\"UserServer\" where username=?", user);
  }

  public static com.aoindustries.aoserv.client.postgresql.User.Name getUserForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.postgresqlUserNameFactory,
        "select username from postgresql.\"UserServer\" where id=?",
        userServer
    );
  }

  public static void invalidateTable(Table.TableId tableId) {
    switch (tableId) {
      case POSTGRES_SERVER_USERS: {
        synchronized (PostgresqlHandler.class) {
          disabledUserServers.clear();
        }
        break;
      }
      case POSTGRES_USERS: {
        synchronized (PostgresqlHandler.class) {
          disabledUsers.clear();
        }
        break;
      }
      default:
        // fall-through
    }
  }

  public static boolean isUserServerDisabled(DatabaseConnection conn, int userServer) throws IOException, SQLException {
    synchronized (PostgresqlHandler.class) {
      Integer i = userServer;
      Boolean o = disabledUserServers.get(i);
      if (o != null) {
        return o;
      }
      boolean isDisabled = getDisableLogForUserServer(conn, userServer) != -1;
      disabledUserServers.put(i, isDisabled);
      return isDisabled;
    }
  }

  public static boolean isUser(DatabaseConnection conn, com.aoindustries.aoserv.client.postgresql.User.Name name) throws IOException, SQLException {
    return conn.queryBoolean(
        "select\n"
            + "  (\n"
            + "    select\n"
            + "      username\n"
            + "    from\n"
            + "      postgresql.\"User\"\n"
            + "    where\n"
            + "      username=?\n"
            + "    limit 1\n"
            + "  ) is not null",
        name
    );
  }

  public static boolean isUserDisabled(DatabaseConnection conn, com.aoindustries.aoserv.client.postgresql.User.Name user) throws IOException, SQLException {
    synchronized (PostgresqlHandler.class) {
      Boolean o = disabledUsers.get(user);
      if (o != null) {
        return o;
      }
      boolean isDisabled = getDisableLogForUser(conn, user) != -1;
      disabledUsers.put(user, isDisabled);
      return isDisabled;
    }
  }

  /**
   * Determines if a PostgreSQL database name is available.
   */
  public static boolean isDatabaseNameAvailable(
      DatabaseConnection conn,
      RequestSource source,
      Database.Name name,
      int postgresqlServer
  ) throws IOException, SQLException {
    int linuxServer = getLinuxServerForServer(conn, postgresqlServer);
    NetHostHandler.checkAccessHost(
        conn,
        source,
        "isPostgresDatabaseNameAvailable",
        linuxServer
    );
    return conn.queryBoolean(
        "select\n"
            + "  (\n"
            + "    select\n"
            + "      id\n"
            + "    from\n"
            + "      postgresql.\"Database\"\n"
            + "    where\n"
            + "      name=?\n"
            + "      and postgres_server=?\n"
            + "    limit 1\n"
            + "  ) is null",
        name,
        postgresqlServer
    );
  }

  /**
   * Determines if a PostgreSQL server name is available.
   */
  public static boolean isServerNameAvailable(
      DatabaseConnection conn,
      RequestSource source,
      com.aoindustries.aoserv.client.postgresql.Server.Name name,
      int linuxServer
  ) throws IOException, SQLException {
    NetHostHandler.checkAccessHost(conn, source, "isServerNameAvailable", linuxServer);
    return conn.queryBoolean("SELECT NOT EXISTS (SELECT * FROM postgresql.\"Server\" WHERE \"name\" = ? AND ao_server = ?)", name, linuxServer);
  }

  public static boolean isUserServerPasswordSet(
      DatabaseConnection conn,
      RequestSource source,
      int userServer
  ) throws IOException, SQLException {
    checkAccessUserServer(conn, source, "isUserServerPasswordSet", userServer);
    com.aoindustries.aoserv.client.postgresql.User.Name user = getUserForUserServer(conn, userServer);
    if (com.aoindustries.aoserv.client.postgresql.User.isSpecial(user)) {
      throw new SQLException("Refusing to check if passwords set on special PostgreSQL user: " + user);
    }
    if (isUserServerDisabled(conn, userServer)) {
      throw new SQLException("Unable to determine if UserServer password is set, account disabled: " + userServer);
    }

    int linuxServer = getLinuxServerForUserServer(conn, userServer);
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    String password = daemonConnector.getPostgresUserPassword(userServer);
    return !com.aoindustries.aoserv.client.postgresql.User.NO_PASSWORD_DB_VALUE.equals(password);
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

    PostgresqlHandler.removeDatabase(conn, invalidateList, database);
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
    if (Database.isSpecial(name)) {
      throw new SQLException("Refusing to remove special PostgreSQL database: " + name);
    }
    // Remove the database entry
    Account.Name account = getAccountForDatabase(conn, database);
    int linuxServer = getLinuxServerForDatabase(conn, database);
    conn.update("delete from postgresql.\"Database\" where id=?", database);

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.POSTGRES_DATABASES,
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

    com.aoindustries.aoserv.client.postgresql.User.Name pu = getUserForUserServer(conn, userServer);
    if (com.aoindustries.aoserv.client.postgresql.User.isSpecial(pu)) {
      throw new SQLException("Refusing to remove special PostgreSQL user: " + pu);
    }

    // Get the details for later use
    int linuxServer = getLinuxServerForUserServer(conn, userServer);
    Account.Name account = getAccountForUserServer(conn, userServer);

    // Make sure that this is not the DBA for any databases
    int count = conn.queryInt("select count(*) from postgresql.\"Database\" where datdba=?", userServer);
    if (count > 0) {
      throw new SQLException("UserServer #" + userServer + " cannot be removed because it is the datdba for " + count + (count == 1 ? " database" : " databases"));
    }

    // Remove the postgres_server_user
    conn.update("delete from postgresql.\"UserServer\" where id=?", userServer);

    // Notify all clients of the updates
    invalidateList.addTable(
        conn,
        Table.TableId.POSTGRES_SERVER_USERS,
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
      com.aoindustries.aoserv.client.postgresql.User.Name user
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
      com.aoindustries.aoserv.client.postgresql.User.Name user
  ) throws IOException, SQLException {
    if (com.aoindustries.aoserv.client.postgresql.User.isSpecial(user)) {
      throw new SQLException("Refusing to remove special PostgreSQL user: " + user);
    }
    Account.Name account = AccountUserHandler.getAccountForUser(conn, user);

    // Remove the postgres_server_user
    IntList linuxServers = conn.queryIntList("select ps.ao_server from postgresql.\"UserServer\" psu, postgresql.\"Server\" ps where psu.username=? and psu.postgres_server = ps.bind", user);
    if (!linuxServers.isEmpty()) {
      conn.update("delete from postgresql.\"UserServer\" where username=?", user);
      invalidateList.addTable(
          conn,
          Table.TableId.POSTGRES_SERVER_USERS,
          account,
          linuxServers,
          false
      );
    }

    // Remove the postgres_user
    conn.update("delete from postgresql.\"User\" where username=?", user);
    invalidateList.addTable(
        conn,
        Table.TableId.POSTGRES_USERS,
        account,
        AccountHandler.getHostsForAccount(conn, account),
        false
    );
  }

  /**
   * Sets a PostgreSQL password.
   */
  public static void setUserServerPassword(
      DatabaseConnection conn,
      RequestSource source,
      int userServer,
      String password
  ) throws IOException, SQLException {
    AccountHandler.checkPermission(conn, source, "setUserServerPassword", Permission.Name.set_postgres_server_user_password);
    checkAccessUserServer(conn, source, "setUserServerPassword", userServer);
    if (isUserServerDisabled(conn, userServer)) {
      throw new SQLException("Unable to set UserServer password, account disabled: " + userServer);
    }

    com.aoindustries.aoserv.client.postgresql.User.Name pu = getUserForUserServer(conn, userServer);
    if (com.aoindustries.aoserv.client.postgresql.User.isSpecial(pu)) {
      throw new SQLException("Refusing to set the password for a special PostgreSQL user: " + pu);
    }

    // Get the server for the user
    int linuxServer = getLinuxServerForUserServer(conn, userServer);

    // Perform the password check here, too.
    if (Objects.equals(password, com.aoindustries.aoserv.client.postgresql.User.NO_PASSWORD)) {
      List<PasswordChecker.Result> results = com.aoindustries.aoserv.client.postgresql.User.checkPassword(pu, password);
      if (PasswordChecker.hasResults(results)) {
        throw new SQLException("Invalid password: " + PasswordChecker.getResultsString(results).replace('\n', '|'));
      }
    }

    // Contact the daemon for the update
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.setPostgresUserPassword(userServer, password);
  }

  public static void setUserServerPredisablePassword(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int userServer,
      String password
  ) throws IOException, SQLException {
    checkAccessUserServer(conn, source, "setUserServerPredisablePassword", userServer);

    com.aoindustries.aoserv.client.postgresql.User.Name pu = getUserForUserServer(conn, userServer);
    if (com.aoindustries.aoserv.client.postgresql.User.isSpecial(pu)) {
      throw new SQLException("May not disable special PostgreSQL user: " + pu);
    }

    if (password == null) {
      if (isUserServerDisabled(conn, userServer)) {
        throw new SQLException("Unable to clear UserServer predisable password, account disabled: " + userServer);
      }
    } else if (!isUserServerDisabled(conn, userServer)) {
      throw new SQLException("Unable to set UserServer predisable password, account not disabled: " + userServer);
    }

    // Update the database
    conn.update(
        "update postgresql.\"UserServer\" set predisable_password=? where id=?",
        password,
        userServer
    );

    invalidateList.addTable(
        conn,
        Table.TableId.POSTGRES_SERVER_USERS,
        getAccountForUserServer(conn, userServer),
        getLinuxServerForUserServer(conn, userServer),
        false
    );
  }

  public static void waitForDatabaseRebuild(
      DatabaseConnection conn,
      RequestSource source,
      int linuxServer
  ) throws IOException, SQLException {
    NetHostHandler.checkAccessHost(conn, source, "waitForDatabaseRebuild", linuxServer);
    NetHostHandler.waitForInvalidates(linuxServer);
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.waitForPostgresDatabaseRebuild();
  }

  public static void waitForServerRebuild(
      DatabaseConnection conn,
      RequestSource source,
      int linuxServer
  ) throws IOException, SQLException {
    NetHostHandler.checkAccessHost(conn, source, "waitForServerRebuild", linuxServer);
    NetHostHandler.waitForInvalidates(linuxServer);
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.waitForPostgresServerRebuild();
  }

  public static void waitForUserRebuild(
      DatabaseConnection conn,
      RequestSource source,
      int linuxServer
  ) throws IOException, SQLException {
    NetHostHandler.checkAccessHost(conn, source, "waitForUserRebuild", linuxServer);
    NetHostHandler.waitForInvalidates(linuxServer);
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.waitForPostgresUserRebuild();
  }

  public static Account.Name getAccountForDatabase(DatabaseConnection conn, int database) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select\n"
            + "  pk.accounting\n"
            + "from\n"
            + "  postgresql.\"Database\" pd,\n"
            + "  postgresql.\"UserServer\" psu,\n"
            + "  account.\"User\" un,\n"
            + "  billing.\"Package\" pk\n"
            + "where\n"
            + "  pd.id=?\n"
            + "  and pd.datdba=psu.id\n"
            + "  and psu.username=un.username\n"
            + "  and un.package=pk.name",
        database
    );
  }

  public static int getPackageForDatabase(DatabaseConnection conn, int database) throws IOException, SQLException {
    return conn.queryInt(
        "select\n"
            + "  pk.id\n"
            + "from\n"
            + "  postgresql.\"Database\" pd,\n"
            + "  postgresql.\"UserServer\" psu,\n"
            + "  account.\"User\" un,\n"
            + "  billing.\"Package\" pk\n"
            + "where\n"
            + "  pd.id=?\n"
            + "  and pd.datdba=psu.id\n"
            + "  and psu.username=un.username\n"
            + "  and un.package=pk.name",
        database
    );
  }

  public static Account.Name getAccountForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select pk.accounting from postgresql.\"UserServer\" psu, account.\"User\" un, billing.\"Package\" pk where psu.username=un.username and un.package=pk.name and psu.id=?",
        userServer
    );
  }

  public static int getLinuxServerForServer(DatabaseConnection conn, int postgresqlServer) throws IOException, SQLException {
    return conn.queryInt("select ao_server from postgresql.\"Server\" where bind = ?", postgresqlServer);
  }

  public static int getPortForServer(DatabaseConnection conn, int postgresqlServer) throws IOException, SQLException {
    return conn.queryInt("select nb.port from postgresql.\"Server\" ps, net.\"Bind\" nb where ps.bind = ? and ps.bind = nb.id", postgresqlServer);
  }

  public static String getMinorVersionForServer(DatabaseConnection conn, int postgresqlServer) throws IOException, SQLException {
    return conn.queryString(
        "SELECT\n"
            + "  pv.minor_version\n"
            + "FROM\n"
            + "  postgresql.\"Server\" ps\n"
            + "  INNER JOIN postgresql.\"Version\" pv ON ps.version = pv.version\n"
            + "WHERE\n"
            + "  ps.bind = ?",
        postgresqlServer
    );
  }

  public static int getServerForDatabase(DatabaseConnection conn, int database) throws IOException, SQLException {
    return conn.queryInt(
        "select postgres_server from postgresql.\"Database\" where id=?",
        database
    );
  }

  public static int getServerForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
    return conn.queryInt("select postgres_server from postgresql.\"UserServer\" where id=?", userServer);
  }

  public static int getLinuxServerForDatabase(DatabaseConnection conn, int database) throws IOException, SQLException {
    return conn.queryInt(
        "SELECT\n"
            + "  ps.ao_server\n"
            + "FROM\n"
            + "  postgresql.\"Database\" pd\n"
            + "  INNER JOIN postgresql.\"Server\" ps ON pd.postgres_server = ps.bind\n"
            + "WHERE\n"
            + "  pd.id=?",
        database
    );
  }

  public static int getDatdbaForDatabase(DatabaseConnection conn, int database) throws IOException, SQLException {
    return conn.queryInt(
        "select\n"
            + "  datdba\n"
            + "from\n"
            + "  postgresql.\"Database\"\n"
            + "where\n"
            + "  id=?",
        database
    );
  }

  public static int getLinuxServerForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
    return conn.queryInt(
        "SELECT\n"
            + "  ps.ao_server\n"
            + "FROM\n"
            + "  postgresql.\"UserServer\" psu\n"
            + "  INNER JOIN postgresql.\"Server\" ps ON psu.postgres_server = ps.bind\n"
            + "WHERE\n"
            + "  psu.id=?",
        userServer
    );
  }

  public static void restartServer(
      DatabaseConnection conn,
      RequestSource source,
      int postgresqlServer
  ) throws IOException, SQLException {
    int linuxServer = getLinuxServerForServer(conn, postgresqlServer);
    boolean canControl = AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_postgresql");
    if (!canControl) {
      throw new SQLException("Not allowed to restart PostgreSQL on " + linuxServer);
    }
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.restartPostgres(postgresqlServer);
  }

  public static void startServer(
      DatabaseConnection conn,
      RequestSource source,
      int postgresqlServer
  ) throws IOException, SQLException {
    int linuxServer = getLinuxServerForServer(conn, postgresqlServer);
    boolean canControl = AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_postgresql");
    if (!canControl) {
      throw new SQLException("Not allowed to start PostgreSQL on " + linuxServer);
    }
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.startPostgresql(postgresqlServer);
  }

  public static void stopServer(
      DatabaseConnection conn,
      RequestSource source,
      int postgresqlServer
  ) throws IOException, SQLException {
    int linuxServer = getLinuxServerForServer(conn, postgresqlServer);
    boolean canControl = AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_postgresql");
    if (!canControl) {
      throw new SQLException("Not allowed to stop PostgreSQL on " + linuxServer);
    }
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.stopPostgresql(postgresqlServer);
  }
}
