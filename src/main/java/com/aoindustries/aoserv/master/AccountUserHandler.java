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

import com.aoapps.collections.IntList;
import com.aoapps.dbc.DatabaseAccess;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.lang.validation.ValidationException;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.account.User;
import com.aoindustries.aoserv.client.schema.Table;
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
public final class AccountUserHandler {

  /** Make no instances. */
  private AccountUserHandler() {
    throw new AssertionError();
  }

  private static final Map<User.Name, Boolean> disabledUsers = new HashMap<>();
  private static final Map<User.Name, Account.Name> userAccounts = new HashMap<>();

  public static boolean canAccessUser(DatabaseConnection conn, RequestSource source, User.Name user) throws IOException, SQLException {
    return PackageHandler.canAccessPackage(conn, source, getPackageForUser(conn, user));
  }

  public static void checkAccessUser(DatabaseConnection conn, RequestSource source, String action, User.Name user) throws IOException, SQLException {
    if (!canAccessUser(conn, source, user)) {
      String message=
        "currentAdministrator="
        +source.getCurrentAdministrator()
        +" is not allowed to access username: action='"
        +action
        +"', username="
        +user
      ;
      throw new SQLException(message);
    }
  }

  public static void addUser(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    Account.Name packageName,
    User.Name name,
    boolean avoidSecurityChecks
  ) throws IOException, SQLException {
    if (name.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) {
      throw new SQLException("Not allowed to add User for name '"+com.aoindustries.aoserv.client.linux.User.MAIL+'\'');
    }

    if (!avoidSecurityChecks) {
      PackageHandler.checkAccessPackage(conn, source, "addUser", packageName);
      if (PackageHandler.isPackageDisabled(conn, packageName)) {
        throw new SQLException("Unable to add User '" + name + "', Package disabled: " + packageName);
      }

      // Make sure people don't create @hostname.com account.User for domains they cannot control
      String usernameStr = name.toString();
      int atPos=usernameStr.lastIndexOf('@');
      if (atPos != -1) {
        String hostname=usernameStr.substring(atPos+1);
        if (hostname.length()>0) {
          MasterServer.checkAccessHostname(conn, source, "addUser", hostname);
        }
      }
    }

    conn.update(
      "insert into account.\"User\" values(?,?,null)",
      name,
      packageName
    );

    // Notify all clients of the update
    Account.Name account = PackageHandler.getAccountForPackage(conn, packageName);
    invalidateList.addTable(conn, Table.TableID.USERNAMES, account, InvalidateList.allHosts, false);
    //invalidateList.addTable(conn, Table.TableID.PACKAGES, accounting, null);
  }

  public static void disableUser(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    int disableLog,
    User.Name user
  ) throws IOException, SQLException {
    AccountHandler.checkAccessDisableLog(conn, source, "disableUser", disableLog, false);
    checkAccessUser(conn, source, "disableUser", user);
    if (isUserDisabled(conn, user)) {
      throw new SQLException("Username is already disabled: "+user);
    }
    String un = user.toString();
    if (com.aoindustries.aoserv.client.linux.User.Name.validate(un).isValid()) {
      com.aoindustries.aoserv.client.linux.User.Name linuxUsername;
      try {
        linuxUsername = com.aoindustries.aoserv.client.linux.User.Name.valueOf(un);
      } catch (ValidationException e) {
        throw new AssertionError("Already validated", e);
      }
      if (
        LinuxAccountHandler.isUser(conn, linuxUsername)
        && !LinuxAccountHandler.isUserDisabled(conn, linuxUsername)
      ) {
        throw new SQLException("Cannot disable Username '"+user+"': Linux user not disabled: "+linuxUsername);
      }
    }
    if (com.aoindustries.aoserv.client.mysql.User.Name.validate(un).isValid()) {
      com.aoindustries.aoserv.client.mysql.User.Name mysqlUsername;
      try {
        mysqlUsername = com.aoindustries.aoserv.client.mysql.User.Name.valueOf(un);
      } catch (ValidationException e) {
        throw new AssertionError("Already validated", e);
      }
      if (
        MysqlHandler.isUser(conn, mysqlUsername)
        && !MysqlHandler.isUserDisabled(conn, mysqlUsername)
      ) {
        throw new SQLException("Cannot disable Username '"+user+"': MySQL user not disabled: "+mysqlUsername);
      }
    }
    if (com.aoindustries.aoserv.client.postgresql.User.Name.validate(un).isValid()) {
      com.aoindustries.aoserv.client.postgresql.User.Name postgresqlUsername;
      try {
        postgresqlUsername = com.aoindustries.aoserv.client.postgresql.User.Name.valueOf(un);
      } catch (ValidationException e) {
        throw new AssertionError("Already validated", e);
      }
      if (
        PostgresqlHandler.isUser(conn, postgresqlUsername)
        && !PostgresqlHandler.isUserDisabled(conn, postgresqlUsername)
      ) {
        throw new SQLException("Cannot disable Username '"+user+"': PostgreSQL user not disabled: "+postgresqlUsername);
      }
    }
    conn.update(
      "update account.\"User\" set disable_log=? where username=?",
      disableLog,
      user
    );

    // Notify all clients of the update
    invalidateList.addTable(
      conn,
      Table.TableID.USERNAMES,
      getAccountForUser(conn, user),
      getHostsForUser(conn, user),
      false
    );
  }

  public static void enableUser(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    User.Name user
  ) throws IOException, SQLException {
    checkAccessUser(conn, source, "enableUser", user);
    int disableLog=getDisableLogForUser(conn, user);
    if (disableLog == -1) {
      throw new SQLException("User is already enabled: "+user);
    }
    AccountHandler.checkAccessDisableLog(conn, source, "enableUser", disableLog, true);
    Account.Name pk=getPackageForUser(conn, user);
    if (PackageHandler.isPackageDisabled(conn, pk)) {
      throw new SQLException("Unable to enable Username '"+user+"', Package not enabled: "+pk);
    }

    conn.update(
      "update account.\"User\" set disable_log=null where username=?",
      user
    );

    // Notify all clients of the update
    invalidateList.addTable(
      conn,
      Table.TableID.USERNAMES,
      getAccountForUser(conn, user),
      getHostsForUser(conn, user),
      false
    );
  }

  public static int getDisableLogForUser(DatabaseConnection conn, User.Name user) throws IOException, SQLException {
    return conn.queryInt("select coalesce(disable_log, -1) from account.\"User\" where username=?", user);
  }

  public static void invalidateTable(Table.TableID tableID) {
    if (tableID == Table.TableID.USERNAMES) {
      synchronized (disabledUsers) {
        disabledUsers.clear();
      }
      synchronized (userAccounts) {
        userAccounts.clear();
      }
    }
  }

  public static boolean isUserNameAvailable(DatabaseConnection conn, User.Name name) throws IOException, SQLException {
    return conn.queryBoolean("select (select username from account.\"User\" where username=?) is null", name);
  }

  public static boolean isUserDisabled(DatabaseConnection conn, User.Name user) throws IOException, SQLException {
    synchronized (disabledUsers) {
      Boolean o = disabledUsers.get(user);
      if (o != null) {
        return o;
      }
      boolean isDisabled = getDisableLogForUser(conn, user) != -1;
      disabledUsers.put(user, isDisabled);
      return isDisabled;
    }
  }

  public static void removeUser(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    User.Name user
  ) throws IOException, SQLException {
    if (user.equals(source.getCurrentAdministrator())) {
      throw new SQLException("Not allowed to remove self: "+user);
    }
    checkAccessUser(conn, source, "removeUser", user);

    removeUser(conn, invalidateList, user);
  }

  public static void removeUser(
    DatabaseConnection conn,
    InvalidateList invalidateList,
    User.Name user
  ) throws IOException, SQLException {
    if (user.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) {
      throw new SQLException("Not allowed to remove User named '"+com.aoindustries.aoserv.client.linux.User.MAIL+'\'');
    }

    Account.Name account = getAccountForUser(conn, user);

    conn.update("delete from account.\"User\" where username=?", user);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableID.USERNAMES, account, InvalidateList.allHosts, false);
  }

  public static Account.Name getAccountForUser(DatabaseAccess db, User.Name user) throws IOException, SQLException {
    synchronized (userAccounts) {
      Account.Name O=userAccounts.get(user);
      if (O != null) {
        return O;
      }
      Account.Name account = db.queryObject(
        ObjectFactories.accountNameFactory,
        "select pk.accounting from account.\"User\" un, billing.\"Package\" pk where un.username=? and un.package=pk.name",
        user
      );
      userAccounts.put(user, account);
      return account;
    }
  }

  // TODO: Cache this lookup, since it is involved iteratively when querying master processes
  public static Account.Name getPackageForUser(DatabaseConnection conn, User.Name user) throws IOException, SQLException {
    return conn.queryObject(
      ObjectFactories.accountNameFactory,
      "select package from account.\"User\" where username=?",
      user
    );
  }

  public static IntList getHostsForUser(DatabaseConnection conn, User.Name user) throws IOException, SQLException {
    return conn.queryIntList(
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
      user
    );
  }

  public static List<User.Name> getUsersForPackage(DatabaseConnection conn, Account.Name packageName) throws IOException, SQLException {
    return conn.queryList(
      ObjectFactories.userNameFactory,
      "select username from account.\"User\" where package=?",
      packageName
    );
  }

  public static boolean canUserAccessHost(DatabaseConnection conn, User.Name user, int host) throws IOException, SQLException {
    return conn.queryBoolean(
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
      user,
      host
    );
  }

  public static void checkUserAccessHost(DatabaseConnection conn, RequestSource source, String action, User.Name user, int host) throws IOException, SQLException {
    if (!canUserAccessHost(conn, user, host)) {
      String message=
      "username="
      +user
      +" is not allowed to access server.id="
      +host
      +": action='"
      +action
      +"'"
      ;
      throw new SQLException(message);
    }
  }
}
