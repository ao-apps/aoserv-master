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
import com.aoapps.collections.SortedArrayList;
import com.aoapps.dbc.DatabaseAccess;
import com.aoapps.dbc.DatabaseAccess.Null;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.lang.Strings;
import com.aoapps.lang.validation.ValidationException;
import com.aoapps.lang.validation.ValidationResult;
import com.aoapps.net.Email;
import com.aoapps.security.HashedPassword;
import com.aoapps.security.UnprotectedPassword;
import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.account.Administrator;
import com.aoindustries.aoserv.client.account.Profile;
import com.aoindustries.aoserv.client.billing.NoticeLog;
import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.password.PasswordChecker;
import com.aoindustries.aoserv.client.payment.CountryCode;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.account.Administrator_GetTableHandler;
import java.io.IOException;
import java.security.SecureRandom;
import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The <code>AccountHandler</code> handles all the accesses to the Account tables.
 *
 * @author  AO Industries, Inc.
 */
public final class AccountHandler {

  /** Make no instances. */
  private AccountHandler() {
    throw new AssertionError();
  }

  private static final Object administratorsLock = new Object();
  private static Map<com.aoindustries.aoserv.client.account.User.Name, Administrator> administrators;

  private static final Object userAccountsLock = new Object();
  private static Map<com.aoindustries.aoserv.client.account.User.Name, List<Account.Name>> userAccounts;

  private static final Map<com.aoindustries.aoserv.client.account.User.Name, Boolean> disabledAdministrators = new HashMap<>();
  private static final Map<Account.Name, Boolean> disabledAccounts = new HashMap<>();

  public static boolean canAccessAccount(DatabaseAccess db, RequestSource source, Account.Name account) throws IOException, SQLException {
    //com.aoindustries.aoserv.client.account.User.Name administrator = source.getAdministrator();
    return
      getAllowedAccounts(db, source)
      .contains(
        account //UsernameHandler.getAccountForUser(conn, administrator)
      )
    ;
  }

  public static boolean canAccessDisableLog(DatabaseConnection conn, RequestSource source, int disableLog, boolean enabling) throws IOException, SQLException {
    com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
    com.aoindustries.aoserv.client.account.User.Name disabledBy = getDisabledByForDisableLog(conn, disableLog);
    if (enabling) {
      Account.Name currentAdministrator_account = AccountUserHandler.getAccountForUser(conn, currentAdministrator);
      Account.Name disabledBy_account = AccountUserHandler.getAccountForUser(conn, disabledBy);
      return isAccountOrParent(conn, currentAdministrator_account, disabledBy_account);
    } else {
      return currentAdministrator.equals(disabledBy);
    }
  }

  public static void cancelAccount(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    Account.Name account,
    String cancelReason
  ) throws IOException, SQLException {
    // Check permissions
    checkPermission(conn, source, "cancelAccount", Permission.Name.cancel_business);

    // Check access to account
    checkAccessAccount(conn, source, "cancelAccount", account);

    if (account.equals(getRootAccount())) {
      throw new SQLException("Not allowed to cancel the root account: " + account);
    }

    // Account must be disabled
    if (!isAccountDisabled(conn, account)) {
      throw new SQLException("Unable to cancel Account, Account not disabled: "+account);
    }

    // Account must not already be canceled
    if (isAccountCanceled(conn, account)) {
      throw new SQLException("Unable to cancel Account, Account already canceled: "+account);
    }

    // May not be the root account
    if (account.equals(getRootAccount())) {
      throw new SQLException("Not allowed to cancel the root account: "+account);
    }

    // May not have any active sub-account
    for (Account.Name childAccount : getChildAccounts(conn, account)) {
      if (!isAccountCanceled(conn, childAccount)) {
        throw new SQLException("Unable to cancel Account, sub-Account not canceled: " + childAccount);
      }
    }

    // Update the database
    conn.update(
      "update account.\"Account\" set canceled=now(), cancel_reason=? where accounting=?",
      cancelReason,
      account
    );

    // Notify the clients
    invalidateList.addTable(conn, Table.TableID.BUSINESSES, account, getHostsForAccount(conn, account), false);
  }

  public static boolean canAccountHost_column(
    DatabaseConnection conn,
    RequestSource source,
    int host,
    String column
  ) throws IOException, SQLException {
    return conn.queryBoolean(
      "select\n"
      + "  bs."+column+"\n"
      + "from\n"
      + "  account.\"User\" un,\n"
      + "  billing.\"Package\" pk,\n"
      + "  account.\"AccountHost\" bs\n"
      + "where\n"
      + "  un.username=?\n"
      + "  and un.package=pk.name\n"
      + "  and pk.accounting=bs.accounting\n"
      + "  and bs.server=?",
      source.getCurrentAdministrator(),
      host
    );
  }

  public static void checkAccessAccount(DatabaseConnection conn, RequestSource source, String action, Account.Name account) throws IOException, SQLException {
    if (!canAccessAccount(conn, source, account)) {
      throw new SQLException(
        "currentAdministrator="
        + source.getCurrentAdministrator()
        + " is not allowed to access account: action='"
        + action
        + "', accounting="
        + account
      );
    }
  }

  public static void checkAccessDisableLog(DatabaseConnection conn, RequestSource source, String action, int disableLog, boolean enabling) throws IOException, SQLException {
    if (!canAccessDisableLog(conn, source, disableLog, enabling)) {
      throw new SQLException(
        "currentAdministrator="
        + source.getCurrentAdministrator()
        + " is not allowed to access account.DisableLog: action='"
        + action
        + "', disableLog="
        + disableLog
      );
    }
  }

  public static void checkAddAccount(DatabaseConnection conn, RequestSource source, String action, Account.Name parent, int host) throws IOException, SQLException {
    boolean canAdd = conn.queryBoolean(
      "select can_add_businesses from account.\"Account\" where accounting=?",
      AccountUserHandler.getAccountForUser(conn, source.getCurrentAdministrator())
    );
    if (canAdd) {
      User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
      if (mu != null) {
        if (MasterServer.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
          canAdd = false;
        }
      } else {
        canAdd =
          canAccessAccount(conn, source, parent)
          && NetHostHandler.canAccessHost(conn, source, host)
        ;
      }
    }
    if (!canAdd) {
      throw new SQLException(
        "currentAdministrator="
        + source.getCurrentAdministrator()
        + " is not allowed to add account: action='"
        + action
        + "', parent="
        + parent
        + ", server="
        + host
      );
    }
  }

  private static final Object cachedPermissionsLock = new Object();
  private static Map<com.aoindustries.aoserv.client.account.User.Name, Set<String>> cachedPermissions;

  public static boolean hasPermission(DatabaseConnection conn, RequestSource source, Permission.Name permission) throws IOException, SQLException {
    synchronized (cachedPermissionsLock) {
      if (cachedPermissions == null) {
        cachedPermissions = conn.queryCall(
          results -> {
            Map<com.aoindustries.aoserv.client.account.User.Name, Set<String>> newCache = new HashMap<>();
            while (results.next()) {
              com.aoindustries.aoserv.client.account.User.Name administrator;
              try {
                administrator = com.aoindustries.aoserv.client.account.User.Name.valueOf(results.getString(1));
              } catch (ValidationException e) {
                throw new SQLException(e);
              }
              Set<String> permissions = newCache.get(administrator);
              if (permissions == null) {
                newCache.put(administrator, permissions = new HashSet<>());
              }
              permissions.add(results.getString(2));
            }
            return newCache;
          },
          "select username, permission from master.\"AdministratorPermission\""
        );
      }
      Set<String> permissions = cachedPermissions.get(source.getCurrentAdministrator());
      return permissions != null && permissions.contains(permission.name());
    }
  }

  public static void checkPermission(DatabaseConnection conn, RequestSource source, String action, Permission.Name permission) throws IOException, SQLException {
    if (!hasPermission(conn, source, permission)) {
      throw new SQLException(
        "currentAdministrator="
        + source.getCurrentAdministrator()
        + " does not have the \""+permission.name()+"\" permission.  Not allowed to make the following call: "
        + action
      );
    }
  }

  public static List<Account.Name> getAllowedAccounts(DatabaseAccess db, RequestSource source) throws IOException, SQLException {
    synchronized (userAccountsLock) {
      com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
      if (userAccounts == null) {
        userAccounts = new HashMap<>();
      }
      List<Account.Name> accounts = userAccounts.get(currentAdministrator);
      if (accounts == null) {
        User mu = MasterServer.getUser(db, currentAdministrator);
        if (mu != null) {
          if (MasterServer.getUserHosts(db, currentAdministrator).length != 0) {
            accounts = db.queryNewCollection(
              AoCollections::newSortedArrayList,
              ObjectFactories.accountNameFactory,
              "select distinct\n"
              + "  bu.accounting\n"
              + "from\n"
              + "  master.\"UserHost\" ms,\n"
              + "  account.\"AccountHost\" bs,\n"
              + "  account.\"Account\" bu\n"
              + "where\n"
              + "  ms.username=?\n"
              + "  and ms.server=bs.server\n"
              + "  and bs.accounting=bu.accounting",
              currentAdministrator
            );
          } else {
            accounts = db.queryNewCollection(
              AoCollections::newSortedArrayList,
              ObjectFactories.accountNameFactory,
              "select accounting from account.\"Account\""
            );
          }
        } else {
          accounts = db.queryNewCollection(
            AoCollections::newSortedArrayList,
            ObjectFactories.accountNameFactory,
            "select\n"
            + "  bu1.accounting\n"
            + "from\n"
            + "  account.\"User\" un,\n"
            + "  billing.\"Package\" pk,\n"
            + TableHandler.BU1_PARENTS_JOIN_NO_COMMA
            + "where\n"
            + "  un.username=?\n"
            + "  and un.package=pk.name\n"
            + "  and (\n"
            + TableHandler.PK_BU1_PARENTS_WHERE
            + "  )",
            currentAdministrator
          );
        }
        userAccounts.put(currentAdministrator, accounts);
      }
      return accounts;
    }
  }

  public static Account.Name getAccountForDisableLog(DatabaseConnection conn, int disableLog) throws IOException, SQLException {
    return conn.queryObject(ObjectFactories.accountNameFactory, "select accounting from account.\"DisableLog\" where id=?", disableLog);
  }

  /**
   * Creates a new <code>Account</code>.
   */
  public static void addAccount(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    Account.Name name,
    String contractVersion,
    // TODO: No longer take a default server, add in a separate step
    int defaultServer,
    Account.Name parent,
    boolean can_add_backup_servers,
    boolean can_add_businesses,
    boolean can_see_prices,
    boolean billParent
  ) throws IOException, SQLException {
    checkAddAccount(conn, source, "addAccount", parent, defaultServer);

    if (isAccountDisabled(conn, parent)) {
      throw new SQLException("Unable to add Account '"+name+"', parent is disabled: "+parent);
    }

    // Must not exceed the maximum account tree depth
    int newDepth=getDepthInAccountTree(conn, parent)+1;
    if (newDepth>Account.MAXIMUM_BUSINESS_TREE_DEPTH) {
      throw new SQLException("Unable to add Account '"+name+"', the maximum depth of the business tree ("+Account.MAXIMUM_BUSINESS_TREE_DEPTH+") would be exceeded.");
    }

    conn.update(
      "insert into account.\"Account\" (\n"
      + "  accounting,\n"
      + "  contract_version,\n"
      + "  parent,\n"
      + "  can_add_backup_server,\n"
      + "  can_add_businesses,\n"
      + "  can_see_prices,\n"
      + "  auto_enable,\n"
      + "  bill_parent\n"
      + ") values(\n"
      + "  ?,\n"
      + "  ?,\n"
      + "  ?,\n"
      + "  ?,\n"
      + "  ?,\n"
      + "  ?,\n"
      + "  true,\n"
      + "  ?\n"
      + ")",
      name,
      contractVersion,
      parent,
      can_add_backup_servers,
      can_add_businesses,
      can_see_prices,
      billParent
    );
    conn.update(
      "insert into account.\"AccountHost\" (\n"
      + "  accounting,\n"
      + "  server,\n"
      + "  is_default,\n"
      + "  can_control_apache,\n"
      + "  can_control_cron,\n"
      + "  can_control_mysql,\n"
      + "  can_control_postgresql,\n"
      + "  can_control_xfs,\n"
      + "  can_control_xvfb,\n"
      + "  can_vnc_console,\n"
      + "  can_control_virtual_server\n"
      + ") values(\n"
      + "  ?,\n"
      + "  ?,\n"
      + "  true,\n"
      + "  false,\n"
      + "  false,\n"
      + "  false,\n"
      + "  false,\n"
      + "  false,\n"
      + "  false,\n"
      + "  false,\n"
      + "  false\n"
      + ")",
      name,
      defaultServer
    );

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableID.BUSINESSES,       InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.BUSINESS_SERVERS, InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.SERVERS,          InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.AO_SERVERS,       InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.VIRTUAL_SERVERS,  InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.NET_DEVICES,      InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.IP_ADDRESSES,     InvalidateList.allAccounts, InvalidateList.allHosts, true);
  }

  /**
   * Creates a new <code>Administrator</code>.
   */
  public static void addAdministrator(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    com.aoindustries.aoserv.client.account.User.Name user,
    String name,
    String title,
    Date birthday,
    boolean isPrivate,
    String workPhone,
    String homePhone,
    String cellPhone,
    String fax,
    String email,
    String address1,
    String address2,
    String city,
    String state,
    String country,
    String zip,
    boolean enableEmailSupport
  ) throws IOException, SQLException {
    AccountUserHandler.checkAccessUser(conn, source, "addAdministrator", user);
    if (user.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) {
      throw new SQLException("Not allowed to add Administrator named '" + com.aoindustries.aoserv.client.linux.User.MAIL + '\'');
    }
    if (country != null && country.equals(CountryCode.US)) {
      state=convertUSState(conn, state);
    }

    String supportCode = enableEmailSupport ? generateSupportCode(conn) : null;
    conn.update(
      "insert into account.\"Administrator\" (\n"
      + "  username,\n"
      + "  name,\n"
      + "  title,\n"
      + "  birthday,\n"
      + "  private,\n"
      + "  work_phone,\n"
      + "  home_phone,\n"
      + "  cell_phone,\n"
      + "  fax,\n"
      + "  email,\n"
      + "  address1,\n"
      + "  address2,\n"
      + "  city,\n"
      + "  state,\n"
      + "  country,\n"
      + "  zip,\n"
      + "  can_switch_users,\n"
      + "  support_code\n"
      + ") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,true,?)",
      user.toString(),
      name,
      title,
      birthday == null ? Null.DATE : birthday,
      isPrivate,
      workPhone,
      homePhone,
      cellPhone,
      fax,
      email,
      address1,
      address2,
      city,
      state,
      country,
      zip,
      supportCode
    );

    // administrators default to having the same permissions as the person who created them
    conn.update(
      "insert into master.\"AdministratorPermission\" (username, permission) select ?, permission from master.\"AdministratorPermission\" where username=?",
      user,
      source.getCurrentAdministrator()
    );

    Account.Name account = AccountUserHandler.getAccountForUser(conn, user);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableID.BUSINESS_ADMINISTRATORS, account, InvalidateList.allHosts, false);
    invalidateList.addTable(conn, Table.TableID.BUSINESS_ADMINISTRATOR_PERMISSIONS, account, InvalidateList.allHosts, false);
  }

  public static String convertUSState(DatabaseConnection conn, String state) throws IOException, SQLException {
    String newState = conn.queryString(
      "select coalesce((select code from account.\"UsState\" where upper(name)=upper(?) or code=upper(?)),'')",
      state,
      state
    );
    if (newState.length() == 0) {
      throw new SQLException(
        state == null || state.length() == 0
        ?"State required for the United States"
        :"Invalid US state: "+state
      );
    }
    return newState;
  }

  /**
   * Creates a new {@link Profile}.
   */
  public static int addProfile(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    Account.Name account,
    String name,
    boolean isPrivate,
    String phone,
    String fax,
    String address1,
    String address2,
    String city,
    String state,
    String country,
    String zip,
    boolean sendInvoice,
    String billingContact,
    Set<Email> billingEmail,
    Profile.EmailFormat billingEmailFormat,
    String technicalContact,
    Set<Email> technicalEmail,
    Profile.EmailFormat technicalEmailFormat
  ) throws IOException, SQLException {
    checkAccessAccount(conn, source, "addProfile", account);

    if (country.equals(CountryCode.US)) {
      state=convertUSState(conn, state);
    }

    int priority = conn.queryInt("select coalesce(max(priority)+1, 1) from account.\"Profile\" where accounting=?", account);

    int profile = conn.updateInt(
      "INSERT INTO account.\"Profile\" VALUES (default,?,?,?,?,?,?,?,?,?,?,?,?,?,now(),?,?,?::account.\"Profile.EmailFormat\",?,?,?::account.\"Profile.EmailFormat\") RETURNING id",
      account.toString(),
      priority,
      name,
      isPrivate,
      phone,
      fax,
      address1,
      address2,
      city,
      state,
      country,
      zip,
      sendInvoice,
      billingContact,
      // TODO: Remove once set table validated
      Strings.join(billingEmail, ", "),
      billingEmailFormat,
      technicalContact,
      // TODO: Remove once set table validated
      Strings.join(technicalEmail, ", "),
      technicalEmailFormat
    );
    short index = 0;
    for (Email email : billingEmail) {
      conn.update(
        "INSERT INTO account.\"Profile.billingEmail{}\" VALUES (?,?,?)",
        profile,
        index++,
        AOServObject.USE_SQL_DATA_WRITE ? email : email.toString()
      );
    }
    index = 0;
    for (Email email : technicalEmail) {
      conn.update(
        "INSERT INTO account.\"Profile.technicalEmail{}\" VALUES (?,?,?)",
        profile,
        index++,
        AOServObject.USE_SQL_DATA_WRITE ? email : email.toString()
      );
    }
    // TODO: Update stored cards since they have "email", "phone", and "fax" from the account profile.
    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableID.BUSINESS_PROFILES, account, InvalidateList.allHosts, false);
    return profile;
  }

  /**
   * Creates a new <code>AccountHost</code>.
   */
  public static int addAccountHost(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    Account.Name account,
    int host
  ) throws IOException, SQLException {
    // Must be allowed to access the Account
    checkAccessAccount(conn, source, "addAccountHost", account);
    if (!account.equals(getRootAccount())) {
      NetHostHandler.checkAccessHost(conn, source, "addAccountHost", host);
    }

    return addAccountHost(conn, invalidateList, account, host);
  }

  /**
   * Creates a new <code>AccountHost</code>.
   */
  public static int addAccountHost(
    DatabaseConnection conn,
    InvalidateList invalidateList,
    Account.Name account,
    int host
  ) throws IOException, SQLException {
    if (isAccountDisabled(conn, account)) {
      throw new SQLException("Unable to add AccountHost, Account disabled: "+account);
    }

    // Parent account must also have access to the server
    if (
      !account.equals(getRootAccount())
      && conn.queryBoolean(
        "select\n"
        + "  (\n"
        + "    select\n"
        + "      bs.id\n"
        + "    from\n"
        + "      account.\"Account\" bu,\n"
        + "      account.\"AccountHost\" bs\n"
        + "    where\n"
        + "      bu.accounting=?\n"
        + "      and bu.parent=bs.accounting\n"
        + "      and bs.server=?\n"
        + "  ) is null",
        account,
        host
      )
    ) {
      throw new SQLException("Unable to add AccountHost, parent does not have access to host.  account="+account+", host="+host);
    }

    boolean hasDefault = conn.queryBoolean("select (select id from account.\"AccountHost\" where accounting=? and is_default limit 1) is not null", account);

    int accountHost = conn.updateInt(
      "INSERT INTO account.\"AccountHost\" (accounting, server, is_default) VALUES (?,?,?) RETURNING id",
      account,
      host,
      !hasDefault
    );

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableID.BUSINESS_SERVERS, InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.SERVERS,          InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.AO_SERVERS,       InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.VIRTUAL_SERVERS,  InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.NET_DEVICES,      InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.IP_ADDRESSES,     InvalidateList.allAccounts, InvalidateList.allHosts, true);
    return accountHost;
  }

  /**
   * Creates a new <code>DistroLog</code>.
   */
  public static int addDisableLog(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    Account.Name account,
    String disableReason
  ) throws IOException, SQLException {
    checkAccessAccount(conn, source, "addDisableLog", account);

    com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
    int disableLog = conn.updateInt(
      "INSERT INTO account.\"DisableLog\" (accounting, disabled_by, disable_reason) VALUES (?,?,?) RETURNING id",
      account,
      currentAdministrator,
      disableReason
    );

    // Notify all clients of the update
    invalidateList.addTable(
      conn,
      Table.TableID.DISABLE_LOG,
      account,
      InvalidateList.allHosts,
      false
    );
    return disableLog;
  }

  /**
   * Adds a notice log.
   */
  public static int addNoticeLog(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    Account.Name account,
    String billingContact,
    String emailAddress,
    String type,
    int transaction
  ) throws IOException, SQLException {
    checkAccessAccount(conn, source, "addNoticeLog", account);
    if (transaction != NoticeLog.NO_TRANSACTION) {
      BillingTransactionHandler.checkAccessTransaction(conn, source, "addNoticeLog", transaction);
    }

    int id = conn.updateInt(
      "INSERT INTO\n"
      + "  billing.\"NoticeLog\"\n"
      + "(\n"
      + "  accounting,\n"
      + "  billing_contact,\n"
      + "  billing_email,\n"
      + "  notice_type,\n"
      + "  transid\n"
      + ") VALUES (\n"
      + "  ?,\n"
      + "  ?,\n"
      + "  ?,\n"
      + "  ?,\n"
      + "  ?\n"
      + ") RETURNING id",
      account.toString(),
      billingContact,
      emailAddress,
      type,
      (transaction == NoticeLog.NO_TRANSACTION) ? Null.INTEGER : transaction
    );
    invalidateList.addTable(conn, Table.TableID.NOTICE_LOG, account, InvalidateList.allHosts, false);

    // Add current balances
    if (
      conn.update(
        "INSERT INTO billing.\"NoticeLog.balance\" (\"noticeLog\", \"balance.currency\", \"balance.value\")\n"
        + "SELECT\n"
        + "  ?,\n"
        + "  ab.currency,\n"
        + "  ab.balance\n"
        + "FROM\n"
        + "  billing.account_balances ab\n"
        + "WHERE\n"
        + "  ab.accounting=?",
        id,
        account.toString()
      ) > 0
    ) {
      invalidateList.addTable(conn, Table.TableID.NoticeLogBalance, account, InvalidateList.allHosts, false);
    }

    return id;
  }

  public static void disableAccount(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    int disableLog,
    Account.Name account
  ) throws IOException, SQLException {
    checkAccessDisableLog(conn, source, "disableAccount", disableLog, false);
    checkAccessAccount(conn, source, "disableAccount", account);
    if (isAccountDisabled(conn, account)) {
      throw new SQLException("Account is already disabled: "+account);
    }
    if (account.equals(getRootAccount())) {
      throw new SQLException("Not allowed to disable the root account: "+account);
    }
    List<Account.Name> packages=getPackagesForAccount(conn, account);
    for (Account.Name packageName : packages) {
      if (!PackageHandler.isPackageDisabled(conn, packageName)) {
        throw new SQLException("Cannot disable Account '"+account+"': Package not disabled: "+packageName);
      }
    }

    conn.update(
      "update account.\"Account\" set disable_log=? where accounting=?",
      disableLog,
      account
    );

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableID.BUSINESSES, account, getHostsForAccount(conn, account), false);
  }

  public static void disableAdministrator(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    int disableLog,
    com.aoindustries.aoserv.client.account.User.Name administrator
  ) throws IOException, SQLException {
    checkAccessDisableLog(conn, source, "disableAdministrator", disableLog, false);
    AccountUserHandler.checkAccessUser(conn, source, "disableAdministrator", administrator);
    if (isAdministratorDisabled(conn, administrator)) {
      throw new SQLException("Administrator is already disabled: " + administrator);
    }

    conn.update(
      "update account.\"Administrator\" set disable_log=? where username=?",
      disableLog,
      administrator
    );

    // Notify all clients of the update
    Account.Name account = AccountUserHandler.getAccountForUser(conn, administrator);
    invalidateList.addTable(conn, Table.TableID.BUSINESS_ADMINISTRATORS, account, getHostsForAccount(conn, account), false);
  }

  public static void enableAccount(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    Account.Name account
  ) throws IOException, SQLException {
    checkAccessAccount(conn, source, "enableAccount", account);
    int disableLog=getDisableLogForAccount(conn, account);
    if (disableLog == -1) {
      throw new SQLException("Account is already enabled: "+account);
    }
    checkAccessDisableLog(conn, source, "enableAccount", disableLog, true);

    if (isAccountCanceled(conn, account)) {
      throw new SQLException("Unable to enable Account, Account canceled: "+account);
    }

    conn.update(
      "update account.\"Account\" set disable_log=null where accounting=?",
      account
    );

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableID.BUSINESSES, account, getHostsForAccount(conn, account), false);
  }

  public static void enableAdministrator(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    com.aoindustries.aoserv.client.account.User.Name administrator
  ) throws IOException, SQLException {
    AccountUserHandler.checkAccessUser(conn, source, "enableAdministrator", administrator);
    int disableLog = getDisableLogForAdministrator(conn, administrator);
    if (disableLog == -1) {
      throw new SQLException("Administrator is already enabled: "+administrator);
    }
    checkAccessDisableLog(conn, source, "enableAdministrator", disableLog, true);

    conn.update(
      "update account.\"Administrator\" set disable_log=null where username=?",
      administrator
    );

    // Notify all clients of the update
    invalidateList.addTable(
      conn,
      Table.TableID.BUSINESS_ADMINISTRATORS,
      AccountUserHandler.getAccountForUser(conn, administrator),
      AccountUserHandler.getHostsForUser(conn, administrator),
      false
    );
  }

  /**
   * Generates a random, unused support code.
   */
  public static String generateSupportCode(DatabaseConnection conn) throws IOException, SQLException {
    SecureRandom secureRandom = MasterServer.getSecureRandom();
    StringBuilder sb = new StringBuilder(11);
    for (int range = 1000000; range < 1000000000; range *= 10) {
      for (int attempt = 0; attempt < 1000; attempt++) {
        sb.setLength(0);
        sb.append((char)('a' + secureRandom.nextInt('z' + 1 - 'a')));
        sb.append((char)('a' + secureRandom.nextInt('z' + 1 - 'a')));
        sb.append(secureRandom.nextInt(range));
        String supportCode = sb.toString();
        if (conn.queryBoolean("select (select support_code from account.\"Administrator\" where support_code=?) is null", supportCode)) {
          return supportCode;
        }
      }
    }
    throw new SQLException("Failed to generate support code after thousands of attempts");
  }

  public static Account.Name generateAccountName(
    DatabaseConnection conn,
    Account.Name template
  ) throws IOException, SQLException {
    // Load the entire list of accounting codes
    Set<Account.Name> codes = conn.queryNewCollection(
      AoCollections::newHashSet,
      ObjectFactories.accountNameFactory,
      "select accounting from account.\"Account\""
    );
    // Find one that is not used
    for (int c=1;c<Integer.MAX_VALUE;c++) {
      Account.Name account;
      try {
        account = Account.Name.valueOf(template.toString()+c);
      } catch (ValidationException e) {
        throw new SQLException(e);
      }
      if (!codes.contains(account)) {
        return account;
      }
    }
    // If could not find one, report and error
    throw new SQLException("Unable to find available accounting code for template: "+template);
  }

  /**
   * Gets the depth of the account in the account tree.  root_accounting is at depth 1.
   *
   * @return  the depth between 1 and Account.MAXIMUM_ACCOUNT_TREE_DEPTH, inclusive.
   */
  public static int getDepthInAccountTree(DatabaseConnection conn, Account.Name account) throws IOException, SQLException {
    int depth=0;
    while (account != null) {
      Account.Name parent=conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select parent from account.\"Account\" where accounting=?",
        account
      );
      depth++;
      account=parent;
    }
    if (depth<1 || depth>Account.MAXIMUM_BUSINESS_TREE_DEPTH) {
      throw new SQLException("Unexpected depth: "+depth);
    }
    return depth;
  }

  public static com.aoindustries.aoserv.client.account.User.Name getDisabledByForDisableLog(DatabaseConnection conn, int disableLog) throws IOException, SQLException {
    return conn.queryObject(
      ObjectFactories.userNameFactory,
      "select disabled_by from account.\"DisableLog\" where id=?",
      disableLog
    );
  }

  public static int getDisableLogForAccount(DatabaseConnection conn, Account.Name account) throws IOException, SQLException {
    return conn.queryInt("select coalesce(disable_log, -1) from account.\"Account\" where accounting=?", account);
  }

  private static final Map<com.aoindustries.aoserv.client.account.User.Name, Integer> administratorDisableLogs = new HashMap<>();
  public static int getDisableLogForAdministrator(DatabaseAccess db, com.aoindustries.aoserv.client.account.User.Name administrator) throws IOException, SQLException {
    synchronized (administratorDisableLogs) {
      if (administratorDisableLogs.containsKey(administrator)) {
        return administratorDisableLogs.get(administrator);
      }
      int disableLog = db.queryInt("select coalesce(disable_log, -1) from account.\"Administrator\" where username=?", administrator);
      administratorDisableLogs.put(administrator, disableLog);
      return disableLog;
    }
  }

  // TODO: Here and all around in AOServ Master, lists are used where objects should be in a unique set
  public static List<Account.Name> getPackagesForAccount(DatabaseConnection conn, Account.Name account) throws IOException, SQLException {
    return conn.queryList(
      ObjectFactories.accountNameFactory,
      "select name from billing.\"Package\" where accounting=?",
      account
    );
  }

  public static IntList getHostsForAccount(DatabaseConnection conn, Account.Name account) throws IOException, SQLException {
    return conn.queryIntList("select server from account.\"AccountHost\" where accounting=?", account);
  }

  public static Account.Name getRootAccount() throws IOException {
    return MasterConfiguration.getRootAccount();
  }

  public static boolean isAccountNameAvailable(
    DatabaseConnection conn,
    Account.Name name
  ) throws IOException, SQLException {
    return conn.queryInt("select count(*) from account.\"Account\" where accounting=?", name) == 0;
  }

  public static boolean isAdministratorPasswordSet(
    DatabaseConnection conn,
    RequestSource source,
    com.aoindustries.aoserv.client.account.User.Name administrator
  ) throws IOException, SQLException {
    AccountUserHandler.checkAccessUser(conn, source, "isAdministratorPasswordSet", administrator);
    return conn.queryBoolean("select (password).algorithm is not null from account.\"Administrator\" where username=?", administrator);
  }

  public static void removeAdministrator(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    com.aoindustries.aoserv.client.account.User.Name administrator
  ) throws IOException, SQLException {
    if (administrator.equals(source.getCurrentAdministrator())) {
      throw new SQLException("Not allowed to remove self: " + administrator);
    }
    AccountUserHandler.checkAccessUser(conn, source, "removeAdministrator", administrator);

    removeAdministrator(conn, invalidateList, administrator);
  }

  public static void removeAdministrator(
    DatabaseConnection conn,
    InvalidateList invalidateList,
    com.aoindustries.aoserv.client.account.User.Name administrator
  ) throws IOException, SQLException {
    if (administrator.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) {
      throw new SQLException("Not allowed to remove Username named '"+com.aoindustries.aoserv.client.linux.User.MAIL+'\'');
    }

    Account.Name account = AccountUserHandler.getAccountForUser(conn, administrator);

    conn.update("delete from master.\"AdministratorPermission\" where username=?", administrator);
    conn.update("delete from account.\"Administrator\" where username=?", administrator);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableID.BUSINESS_ADMINISTRATORS, account, InvalidateList.allHosts, false);
  }

  /**
   * Removes a <code>AccountHost</code>.
   */
  public static void removeAccountHost(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    int accountHost
  ) throws IOException, SQLException {
    Account.Name account = conn.queryObject(
      ObjectFactories.accountNameFactory,
      "select accounting from account.\"AccountHost\" where id=?",
      accountHost
    );
    int host = conn.queryInt("select server from account.\"AccountHost\" where id=?", accountHost);

    // Must be allowed to access this Account
    checkAccessAccount(conn, source, "removeAccountHost", account);

    // Do not remove the default unless it is the only one left
    if (
      conn.queryBoolean("select is_default from account.\"AccountHost\" where id=?", accountHost)
      && conn.queryInt("select count(*) from account.\"AccountHost\" where accounting=?", account)>1
    ) {
      throw new SQLException("Cannot remove the default AccountHost unless it is the last AccountHost for an account: " + accountHost);
    }

    removeAccountHost(
      conn,
      invalidateList,
      accountHost
    );
  }

  /**
   * Removes a <code>AccountHost</code>.
   */
  public static void removeAccountHost(
    DatabaseConnection conn,
    InvalidateList invalidateList,
    int accountHost
  ) throws IOException, SQLException {
    Account.Name account = conn.queryObject(
      ObjectFactories.accountNameFactory,
      "select accounting from account.\"AccountHost\" where id=?",
      accountHost
    );
    int host = conn.queryInt("select server from account.\"AccountHost\" where id=?", accountHost);

    // No children should be able to access the server
    if (
      conn.queryBoolean(
        "select\n"
        + "  (\n"
        + "    select\n"
        + "      bs.id\n"
        + "    from\n"
        + "      account.\"Account\" bu,\n"
        + "      account.\"AccountHost\" bs\n"
        + "    where\n"
        + "      bu.parent=?\n"
        + "      and bu.accounting=bs.accounting\n"
        + "      and bs.server=?\n"
        + "    limit 1\n"
        + "  ) is not null",
        account,
        host
      )
    ) {
      throw new SQLException("Account="+account+" still has at least one child Account able to access Host="+host);
    }

    /*
     * Account must not have any resources on the server
     */
    // email.Pipe
    if (
      conn.queryBoolean(
        "select\n"
        + "  (\n"
        + "    select\n"
        + "      ep.id\n"
        + "    from\n"
        + "      billing.\"Package\" pk,\n"
        + "      email.\"Pipe\" ep\n"
        + "    where\n"
        + "      pk.accounting=?\n"
        + "      and pk.name=ep.package\n"
        + "      and ep.ao_server=?\n"
        + "    limit 1\n"
        + "  )\n"
        + "  is not null\n",
        account,
        host
      )
    ) {
      throw new SQLException("Account="+account+" still owns at least one Pipe on Host="+host);
    }

    // web.Site
    if (
      conn.queryBoolean(
        "select\n"
        + "  (\n"
        + "    select\n"
        + "      hs.id\n"
        + "    from\n"
        + "      billing.\"Package\" pk,\n"
        + "      web.\"Site\" hs\n"
        + "    where\n"
        + "      pk.accounting=?\n"
        + "      and pk.name=hs.package\n"
        + "      and hs.ao_server=?\n"
        + "    limit 1\n"
        + "  )\n"
        + "  is not null\n",
        account,
        host
      )
    ) {
      throw new SQLException("Account="+account+" still owns at least one Site on Host="+host);
    }

    // net.IpAddress
    if (
      conn.queryBoolean(
        "select\n"
        + "  (\n"
        + "    select\n"
        + "      ia.id\n"
        + "    from\n"
        + "      billing.\"Package\" pk,\n"
        + "      net.\"IpAddress\" ia,\n"
        + "      net.\"Device\" nd\n"
        + "    where\n"
        + "      pk.accounting=?\n"
        + "      and pk.id=ia.package\n"
        + "      and ia.device=nd.id\n"
        + "      and nd.server=?\n"
        + "    limit 1\n"
        + "  )\n"
        + "  is not null\n",
        account,
        host
      )
    ) {
      throw new SQLException("Account="+account+" still owns at least one net.IpAddress on Host="+host);
    }

    // linux.UserServer
    if (
      conn.queryBoolean(
        "select\n"
        + "  (\n"
        + "    select\n"
        + "      lsa.id\n"
        + "    from\n"
        + "      billing.\"Package\" pk,\n"
        + "      account.\"User\" un,\n"
        + "      linux.\"UserServer\" lsa\n"
        + "    where\n"
        + "      pk.accounting=?\n"
        + "      and pk.name=un.package\n"
        + "      and un.username=lsa.username\n"
        + "      and lsa.ao_server=?\n"
        + "    limit 1\n"
        + "  )\n"
        + "  is not null\n",
        account,
        host
      )
    ) {
      throw new SQLException("Account="+account+" still owns at least one UserServer on Host="+host);
    }

    // linux.GroupServer
    if (
      conn.queryBoolean(
        "select\n"
        + "  (\n"
        + "    select\n"
        + "      lsg.id\n"
        + "    from\n"
        + "      billing.\"Package\" pk,\n"
        + "      linux.\"Group\" lg,\n"
        + "      linux.\"GroupServer\" lsg\n"
        + "    where\n"
        + "      pk.accounting=?\n"
        + "      and pk.name=lg.package\n"
        + "      and lg.name=lsg.name\n"
        + "      and lsg.ao_server=?\n"
        + "    limit 1\n"
        + "  )\n"
        + "  is not null\n",
        account,
        host
      )
    ) {
      throw new SQLException("Account="+account+" still owns at least one GroupServer on Host="+host);
    }

    // mysql.Database
    if (
      conn.queryBoolean(
        "select\n"
        + "  (\n"
        + "    select\n"
        + "      md.id\n"
        + "    from\n"
        + "      billing.\"Package\" pk,\n"
        + "      mysql.\"Database\" md,\n"
        + "      mysql.\"Server\" ms\n"
        + "    where\n"
        + "      pk.accounting=?\n"
        + "      and pk.name=md.package\n"
        + "      and md.mysql_server=ms.bind\n"
        + "      and ms.ao_server=?\n"
        + "    limit 1\n"
        + "  )\n"
        + "  is not null\n",
        account,
        host
      )
    ) {
      throw new SQLException("Account="+account+" still owns at least one Database on Host="+host);
    }

    // mysql.UserServer
    if (
      conn.queryBoolean(
        "select\n"
        + "  (\n"
        + "    select\n"
        + "      msu.id\n"
        + "    from\n"
        + "      billing.\"Package\" pk,\n"
        + "      account.\"User\" un,\n"
        + "      mysql.\"UserServer\" msu,\n"
        + "      mysql.\"Server\" ms\n"
        + "    where\n"
        + "      pk.accounting=?\n"
        + "      and pk.name=un.package\n"
        + "      and un.username=msu.username\n"
        + "      and msu.mysql_server=ms.bind\n"
        + "      and ms.ao_server=?\n"
        + "    limit 1\n"
        + "  )\n"
        + "  is not null\n",
        account,
        host
      )
    ) {
      throw new SQLException("Account="+account+" still owns at least one UserServer on Host="+host);
    }

    // net.Bind
    if (
      conn.queryBoolean(
        "select\n"
        + "  (\n"
        + "    select\n"
        + "      nb.id\n"
        + "    from\n"
        + "      billing.\"Package\" pk,\n"
        + "      net.\"Bind\" nb\n"
        + "    where\n"
        + "      pk.accounting=?\n"
        + "      and pk.name=nb.package\n"
        + "      and nb.server=?\n"
        + "    limit 1\n"
        + "  )\n"
        + "  is not null\n",
        account,
        host
      )
    ) {
      throw new SQLException("Account="+account+" still owns at least one Bind on Host="+host);
    }

    // postgresql.Database
    if (
      conn.queryBoolean(
        "select\n"
        + "  (\n"
        + "    select\n"
        + "      pd.id\n"
        + "    from\n"
        + "      billing.\"Package\" pk,\n"
        + "      account.\"User\" un,\n"
        + "      postgresql.\"Server\" ps,\n"
        + "      postgresql.\"UserServer\" psu,\n"
        + "      postgresql.\"Database\" pd\n"
        + "    where\n"
        + "      pk.accounting=?\n"
        + "      and pk.name=un.package\n"
        + "      and ps.ao_server=?\n"
        + "      and un.username=psu.username and ps.bind = psu.postgres_server\n"
        + "      and pd.datdba=psu.id\n"
        + "    limit 1\n"
        + "  )\n"
        + "  is not null\n",
        account,
        host
      )
    ) {
      throw new SQLException("Account="+account+" still owns at least one Database on Host="+host);
    }

    // postgresql.UserServer
    if (
      conn.queryBoolean(
        "select\n"
        + "  (\n"
        + "    select\n"
        + "      psu.id\n"
        + "    from\n"
        + "      billing.\"Package\" pk,\n"
        + "      account.\"User\" un,\n"
        + "      postgresql.\"Server\" ps,\n"
        + "      postgresql.\"UserServer\" psu\n"
        + "    where\n"
        + "      pk.accounting=?\n"
        + "      and pk.name=un.package\n"
        + "      and ps.ao_server=?\n"
        + "      and un.username=psu.username and ps.bind = psu.postgres_server\n"
        + "    limit 1\n"
        + "  )\n"
        + "  is not null\n",
        account,
        host
      )
    ) {
      throw new SQLException("Account="+account+" still owns at least one UserServer on Host="+host);
    }

    // email.Domain
    if (
      conn.queryBoolean(
        "select\n"
        + "  (\n"
        + "    select\n"
        + "      ed.id\n"
        + "    from\n"
        + "      billing.\"Package\" pk,\n"
        + "      email.\"Domain\" ed\n"
        + "    where\n"
        + "      pk.accounting=?\n"
        + "      and pk.name=ed.package\n"
        + "      and ed.ao_server=?\n"
        + "    limit 1\n"
        + "  )\n"
        + "  is not null\n",
        account,
        host
      )
    ) {
      throw new SQLException("Account="+account+" still owns at least one Domain on Host="+host);
    }

    // email.SmtpRelay
    if (
      conn.queryBoolean(
        "select\n"
        + "  (\n"
        + "    select\n"
        + "      esr.id\n"
        + "    from\n"
        + "      billing.\"Package\" pk,\n"
        + "      email.\"SmtpRelay\" esr\n"
        + "    where\n"
        + "      pk.accounting=?\n"
        + "      and pk.name=esr.package\n"
        + "      and esr.ao_server is not null\n"
        + "      and esr.ao_server=?\n"
        + "    limit 1\n"
        + "  )\n"
        + "  is not null\n",
        account,
        host
      )
    ) {
      throw new SQLException("Account="+account+" still owns at least one SmtpRelay on Host="+host);
    }

    conn.update("delete from account.\"AccountHost\" where id=?", accountHost);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableID.BUSINESS_SERVERS, InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.SERVERS, InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.AO_SERVERS, InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.VIRTUAL_SERVERS, InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.NET_DEVICES, InvalidateList.allAccounts, InvalidateList.allHosts, true);
    invalidateList.addTable(conn, Table.TableID.IP_ADDRESSES, InvalidateList.allAccounts, InvalidateList.allHosts, true);
  }

  public static void removeDisableLog(
    DatabaseConnection conn,
    InvalidateList invalidateList,
    int disableLog
  ) throws IOException, SQLException {
    Account.Name account = getAccountForDisableLog(conn, disableLog);

    conn.update("delete from account.\"DisableLog\" where id=?", disableLog);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableID.DISABLE_LOG, account, InvalidateList.allHosts, false);
  }

  public static void setAccountName(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    Account.Name account,
    Account.Name name
  ) throws IOException, SQLException {
    checkAccessAccount(conn, source, "setAccountName", account);

    conn.update("update account.\"Account\" set accounting=? where accounting=?", name, account);

    // TODO: Update stored cards since they have "group_name" meta data matching the account name.

    // Notify all clients of the update
    Collection<Account.Name> accts=InvalidateList.getAccountCollection(account, name);
    invalidateList.addTable(conn, Table.TableID.BUSINESSES, accts, InvalidateList.allHosts, false);
    invalidateList.addTable(conn, Table.TableID.BUSINESS_PROFILES, accts, InvalidateList.allHosts, false);
    invalidateList.addTable(conn, Table.TableID.BUSINESS_SERVERS, accts, InvalidateList.allHosts, false);
    invalidateList.addTable(conn, Table.TableID.CREDIT_CARDS, accts, InvalidateList.allHosts, false);
    invalidateList.addTable(conn, Table.TableID.DISABLE_LOG, accts, InvalidateList.allHosts, false);
    invalidateList.addTable(conn, Table.TableID.MONTHLY_CHARGES, accts, InvalidateList.allHosts, false);
    invalidateList.addTable(conn, Table.TableID.NOTICE_LOG, accts, InvalidateList.allHosts, false);
    invalidateList.addTable(conn, Table.TableID.PACKAGE_DEFINITIONS, accts, InvalidateList.allHosts, false);
    invalidateList.addTable(conn, Table.TableID.PACKAGES, accts, InvalidateList.allHosts, false);
    invalidateList.addTable(conn, Table.TableID.SERVERS, accts, InvalidateList.allHosts, false);
    invalidateList.addTable(conn, Table.TableID.TICKETS, accts, InvalidateList.allHosts, false);
    invalidateList.addTable(conn, Table.TableID.TRANSACTIONS, accts, InvalidateList.allHosts, false);
  }

  /**
   * @param password  Is destroyed before this method returns.  If the original password is
   *                  needed, pass a clone to this method.
   */
  public static void setAdministratorPassword(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    com.aoindustries.aoserv.client.account.User.Name administrator,
    UnprotectedPassword password
  ) throws IOException, SQLException {
    try {
      // An administrator may always reset their own passwords
      if (!administrator.equals(source.getCurrentAdministrator())) {
        checkPermission(conn, source, "setAdministratorPassword", Permission.Name.set_business_administrator_password);
      }

      AccountUserHandler.checkAccessUser(conn, source, "setAdministratorPassword", administrator);

      if (password != null) {
        // Perform the password check here, too.
        List<PasswordChecker.Result> results = password.invoke(chars -> Administrator.checkPassword(administrator, new String(chars)));
        if (PasswordChecker.hasResults(results)) {
          throw new SQLException("Invalid password: " + PasswordChecker.getResultsString(results).replace('\n', '|'));
        }
      }

      setAdministratorPassword(conn, invalidateList, administrator, password);
    } finally {
      if (password != null) {
        password.destroy();
      }
    }
  }

  /**
   * @param password  Is destroyed before this method returns.  If the original password is
   *                  needed, pass a clone to this method.
   */
  public static void setAdministratorPassword(
    DatabaseAccess db,
    InvalidateList invalidateList,
    com.aoindustries.aoserv.client.account.User.Name administrator,
    UnprotectedPassword password
  ) throws IOException, SQLException {
    try {
      if (administrator.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) {
        throw new SQLException("Not allowed to set password for Administrator named '"+com.aoindustries.aoserv.client.linux.User.MAIL+'\'');
      }

      if (isAdministratorDisabled(db, administrator)) {
        throw new SQLException("Unable to set password, Administrator disabled: "+administrator);
      }

      HashedPassword encrypted = (password == null)
        ? HashedPassword.NO_PASSWORD
        : new HashedPassword(password);
      db.update(
        "update account.\"Administrator\" set password=ROW(?,?,?,?) where username=?",
        encrypted.getAlgorithm() == null ? Null.VARCHAR   : encrypted.getAlgorithm().getAlgorithmName(),
        encrypted.getSalt()      == null ? Null.VARBINARY : encrypted.getSalt(),
        encrypted.getIterations(),
        encrypted.getHash()      == null ? Null.VARBINARY : encrypted.getHash(),
        administrator
      );

      // Notify all clients of the update
      invalidateList.addTable(
        db,
        Table.TableID.BUSINESS_ADMINISTRATORS,
        AccountUserHandler.getAccountForUser(db, administrator),
        InvalidateList.allHosts,
        false
      );
    } finally {
      if (password != null) {
        password.destroy();
      }
    }
  }

  /**
   * Sets an administrators profile.
   */
  // TODO: Versioned profiles list on Account
  public static void setAdministratorProfile(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    com.aoindustries.aoserv.client.account.User.Name administrator,
    String name,
    String title,
    Date birthday,
    boolean isPrivate,
    String workPhone,
    String homePhone,
    String cellPhone,
    String fax,
    String email,
    String address1,
    String address2,
    String city,
    String state,
    String country,
    String zip
  ) throws IOException, SQLException {
    AccountUserHandler.checkAccessUser(conn, source, "setAdministratorProfile", administrator);
    if (administrator.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) {
      throw new SQLException("Not allowed to set Administrator profile for user '"+com.aoindustries.aoserv.client.linux.User.MAIL+'\'');
    }

    ValidationResult emailResult = Email.validate(email);
    if (!emailResult.isValid()) {
      throw new SQLException("Invalid format for email: " + emailResult);
    }

    if (country != null && country.equals(CountryCode.US)) {
      state=convertUSState(conn, state);
    }

    Account.Name account = AccountUserHandler.getAccountForUser(conn, administrator);
    conn.update(
      "update account.\"Administrator\" set name=?, title=?, birthday=?, private=?, work_phone=?, home_phone=?, cell_phone=?, fax=?, email=?, address1=?, address2=?, city=?, state=?, country=?, zip=? where username=?",
      name,
      title,
      birthday == null?Null.DATE:birthday,
      isPrivate,
      workPhone,
      homePhone,
      cellPhone,
      fax,
      email,
      address1,
      address2,
      city,
      state,
      country,
      zip,
      administrator
    );

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableID.BUSINESS_ADMINISTRATORS, account, InvalidateList.allHosts, false);
  }

  /**
   * Sets the default Host for a Account
   */
  public static void setDefaultAccountHost(
    DatabaseConnection conn,
    RequestSource source,
    InvalidateList invalidateList,
    int accountHost
  ) throws IOException, SQLException {
    Account.Name account = conn.queryObject(
      ObjectFactories.accountNameFactory,
      "select accounting from account.\"AccountHost\" where id=?",
      accountHost
    );

    checkAccessAccount(conn, source, "setDefaultAccountHost", account);

    if (isAccountDisabled(conn, account)) {
      throw new SQLException("Unable to set the default AccountHost, Account disabled: "+account);
    }

    // Update the table
    conn.update(
      "update account.\"AccountHost\" set is_default=true where id=?",
      accountHost
    );
    conn.update(
      "update account.\"AccountHost\" set is_default=false where accounting=? and id != ?",
      account,
      accountHost
    );

    // Notify all clients of the update
    invalidateList.addTable(conn,
      Table.TableID.BUSINESS_SERVERS,
      account,
      InvalidateList.allHosts,
      false
    );
  }

  public static Administrator getAdministrator(DatabaseAccess db, com.aoindustries.aoserv.client.account.User.Name user) throws IOException, SQLException {
    synchronized (administratorsLock) {
      if (administrators == null) {
        administrators = db.queryCall(
          results -> {
            Map<com.aoindustries.aoserv.client.account.User.Name, Administrator> table=new HashMap<>();
            while (results.next()) {
              Administrator ba=new Administrator();
              ba.init(results);
              table.put(ba.getKey(), ba);
            }
            return table;
          },
          Administrator_GetTableHandler.MASTER_QUERY
        );
      }
      return administrators.get(user);
    }
  }

  public static void invalidateTable(Table.TableID tableID) {
    if (tableID == Table.TableID.BUSINESS_ADMINISTRATORS) {
      synchronized (administratorsLock) {
        administrators=null;
      }
      synchronized (disabledAdministrators) {
        disabledAdministrators.clear();
      }
      synchronized (administratorDisableLogs) {
        administratorDisableLogs.clear();
      }
    } else if (tableID == Table.TableID.BUSINESSES) {
      synchronized (userAccountsLock) {
        userAccounts=null;
      }
      synchronized (disabledAccounts) {
        disabledAccounts.clear();
      }
    } else if (tableID == Table.TableID.BUSINESS_ADMINISTRATOR_PERMISSIONS) {
      synchronized (cachedPermissionsLock) {
        cachedPermissions = null;
      }
    }
  }

  public static Account.Name getParentAccount(DatabaseConnection conn, Account.Name account) throws IOException, SQLException {
    return conn.queryObject(
      ObjectFactories.accountNameFactory,
      "select parent from account.\"Account\" where accounting=?",
      account
    );
  }

  // TODO: Many uses of List should be Set when the elements are unique
  public static List<Account.Name> getChildAccounts(DatabaseConnection conn, Account.Name account) throws IOException, SQLException {
    return conn.queryList(
      ObjectFactories.accountNameFactory,
      "select accounting from account.\"Account\" where parent=?",
      account
    );
  }

  // TODO: Seems unused 20181218
  public static Set<Email> getTechnicalEmail(DatabaseConnection conn, Account.Name account) throws IOException, SQLException {
    return conn.queryNewCollection(
      AoCollections::newLinkedHashSet,
      ObjectFactories.emailFactory,
      "SELECT\n"
      + "  technical_email\n"
      + "FROM\n"
      + "  account.\"Profile\"\n"
      + "WHERE\n"
      + "  accounting=?\n"
      + "ORDER BY\n"
      + "  priority DESC\n"
      + "LIMIT 1",
      account
    );
  }

  public static boolean isAdministrator(DatabaseAccess db, com.aoindustries.aoserv.client.account.User.Name user) throws IOException, SQLException {
    return getAdministrator(db, user) != null;
  }

  public static boolean isAdministratorDisabled(DatabaseAccess db, com.aoindustries.aoserv.client.account.User.Name administrator) throws IOException, SQLException {
    Boolean o;
    synchronized (disabledAdministrators) {
      o = disabledAdministrators.get(administrator);
    }
    if (o != null) {
      return o;
    }
    boolean isDisabled = getDisableLogForAdministrator(db, administrator) != -1;
    synchronized (disabledAdministrators) {
      disabledAdministrators.put(administrator, isDisabled);
    }
    return isDisabled;
  }

  public static boolean isAccountDisabled(DatabaseConnection conn, Account.Name account) throws IOException, SQLException {
    synchronized (disabledAccounts) {
      Boolean o = disabledAccounts.get(account);
      if (o != null) {
        return o;
      }
      boolean isDisabled = getDisableLogForAccount(conn, account) != -1;
      disabledAccounts.put(account, isDisabled);
      return isDisabled;
    }
  }

  public static boolean isAccountCanceled(DatabaseConnection conn, Account.Name account) throws IOException, SQLException {
    return conn.queryBoolean("select canceled is not null from account.\"Account\" where accounting=?", account);
  }

  public static boolean isAccountBillParent(DatabaseConnection conn, Account.Name account) throws IOException, SQLException {
    return conn.queryBoolean("select bill_parent from account.\"Account\" where accounting=?", account);
  }

  public static boolean canSeePrices(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
    return canSeePrices(conn, AccountUserHandler.getAccountForUser(conn, source.getCurrentAdministrator()));
  }

  public static boolean canSeePrices(DatabaseConnection conn, Account.Name account) throws IOException, SQLException {
    return conn.queryBoolean("select can_see_prices from account.\"Account\" where accounting=?", account);
  }

  public static boolean isAccountOrParent(DatabaseConnection conn, Account.Name parentAccounting, Account.Name account) throws IOException, SQLException {
    return conn.queryBoolean("select account.is_account_or_parent(?,?)", parentAccounting, account);
  }

  public static boolean canSwitchUser(DatabaseAccess db, com.aoindustries.aoserv.client.account.User.Name authenticatedAs, com.aoindustries.aoserv.client.account.User.Name connectAs) throws IOException, SQLException {
    Account.Name authAccounting=AccountUserHandler.getAccountForUser(db, authenticatedAs);
    Account.Name connectAccounting=AccountUserHandler.getAccountForUser(db, connectAs);
    // Cannot switch within same account
    if (authAccounting.equals(connectAccounting)) {
      return false;
    }
    return db.queryBoolean(
      "select\n"
      + "  (select can_switch_users from account.\"Administrator\" where username=?)\n"
      + "  and account.is_account_or_parent(?,?)",
      authenticatedAs,
      authAccounting,
      connectAccounting
    );
  }

  /**
   * Gets the list of both technical and billing contacts for all not-canceled account.Account.
   *
   * @return  a <code>HashMap</code> of <code>ArrayList</code>
   */
  public static Map<Account.Name, List<String>> getAccountContacts(DatabaseConnection conn) throws IOException, SQLException {
    return conn.queryCall(
      results -> {
        // Load the list of account.Account and their contacts
        Map<Account.Name, List<String>> accountContacts = new HashMap<>();
        List<String> foundAddresses = new SortedArrayList<>();
        try {
          while (results.next()) {
            Account.Name account = Account.Name.valueOf(results.getString(1));
            if (!accountContacts.containsKey(account)) {
              List<String> uniqueAddresses=new ArrayList<>();
              foundAddresses.clear();
              // billing contacts
              List<String> addresses=Strings.splitCommaSpace(results.getString(2));
              for (String address : addresses) {
                String addy = address.toLowerCase();
                if (!foundAddresses.contains(addy)) {
                  uniqueAddresses.add(addy);
                  foundAddresses.add(addy);
                }
              }
              // technical contacts
              addresses=Strings.splitCommaSpace(results.getString(3));
              for (String address : addresses) {
                String addy = address.toLowerCase();
                if (!foundAddresses.contains(addy)) {
                  uniqueAddresses.add(addy);
                  foundAddresses.add(addy);
                }
              }
              accountContacts.put(account, uniqueAddresses);
            }
          }
        } catch (ValidationException e) {
          throw new SQLException(e.getLocalizedMessage(), e);
        }
        return accountContacts;
      },
      "select bp.accounting, bp.billing_email, bp.technical_email from account.\"Profile\" bp, account.\"Account\" bu where bp.accounting=bu.accounting and bu.canceled is null order by bp.accounting, bp.priority desc"
    );
  }

  /**
   * Gets the best estimate of an account for a list of email addresses or <code>null</code> if can't determine.
   * The algorithm takes these steps.
   * <ol>
   *   <li>Look for exact matches in billing and technical contacts, with a weight of 10.</li>
   *   <li>Look for matches in <code>email.Domain</code>, with a weight of 5</li>
   *   <li>Look for matches in <code>web.VirtualHostName</code> with a weight of 1</li>
   *   <li>Look for matches in <code>dns.Zone</code> with a weight of 1</li>
   *   <li>Add up the weights per account</li>
   *   <li>Find the highest weight</li>
   *   <li>Follow the bill_parents up to top billing level</li>
   * </ol>
   */
  public static Account.Name getAccountFromEmailAddresses(DatabaseConnection conn, List<String> addresses) throws IOException, SQLException {
    // Load the list of account.Account and their contacts
    Map<Account.Name, List<String>> accountContacts = getAccountContacts(conn);

    // The cumulative weights are added up here, per account
    Map<Account.Name, Integer> accountWeights = new HashMap<>();

    // Go through all addresses
    for (String address : addresses) {
      String addy = address.toLowerCase();
      // Look for billing and technical contact matches, 10 points each
      Iterator<Account.Name> I=accountContacts.keySet().iterator();
      while (I.hasNext()) {
        Account.Name account = I.next();
        List<String> list=accountContacts.get(account);
        for (String contact : list) {
          if (addy.equals(contact)) {
            addWeight(accountWeights, account, 10);
          }
        }
      }

      // Parse the domain
      int pos=addy.lastIndexOf('@');
      if (pos != -1) {
        String domain=addy.substring(pos+1);
        if (domain.length()>0) {
          // Look for matches in email.Domain, 5 points each
          List<Account.Name> domain_accounts = conn.queryList(
            ObjectFactories.accountNameFactory,
            "select\n"
            + "  pk.accounting\n"
            + "from\n"
            + "  email.\"Domain\" ed,\n"
            + "  billing.\"Package\" pk\n"
            + "where\n"
            + "  ed.domain=?\n"
            + "  and ed.package=pk.name",
            domain
          );
          for (Account.Name account : domain_accounts) {
            addWeight(accountWeights, account, 5);
          }
          // Look for matches in web.VirtualHostName, 1 point each
          List<Account.Name> site_accounts = conn.queryList(
            ObjectFactories.accountNameFactory,
            "select\n"
            + "  pk.accounting\n"
            + "from\n"
            + "  web.\"VirtualHostName\" hsu,\n"
            + "  web.\"VirtualHost\" hsb,\n"
            + "  web.\"Site\" hs,\n"
            + "  billing.\"Package\" pk\n"
            + "where\n"
            + "  hsu.hostname=?\n"
            + "  and hsu.httpd_site_bind=hsb.id\n"
            + "  and hsb.httpd_site=hs.id\n"
            + "  and hs.package=pk.name",
            domain
          );
          for (Account.Name account : site_accounts) {
            addWeight(accountWeights, account, 1);
          }
          // Look for matches in dns.Zone, 1 point each
          List<Account.Name> zone_accounts = conn.queryList(
            ObjectFactories.accountNameFactory,
            "select\n"
            + "  pk.accounting\n"
            + "from\n"
            + "  dns.\"Zone\" dz,\n"
            + "  billing.\"Package\" pk\n"
            + "where\n"
            + "  dz.zone=?\n"
            + "  and dz.package=pk.name",
            domain
          );
          for (Account.Name account : zone_accounts) {
            addWeight(accountWeights, account, 1);
          }
        }
      }
    }

    // Find the highest weight
    Iterator<Account.Name> I=accountWeights.keySet().iterator();
    int highest=0;
    Account.Name highestAccounting=null;
    while (I.hasNext()) {
      Account.Name account = I.next();
      int weight=accountWeights.get(account);
      if (weight>highest) {
        highest=weight;
        highestAccounting=account;
      }
    }

    // Follow the bill_parent flags toward the top, but skipping canceled
    while (
      highestAccounting != null
      && (
        isAccountCanceled(conn, highestAccounting)
        || isAccountBillParent(conn, highestAccounting)
      )
    ) {
      highestAccounting=getParentAccount(conn, highestAccounting);
    }

    // Do not accept root account
    if (highestAccounting != null && highestAccounting.equals(getRootAccount())) {
      highestAccounting=null;
    }

    // Return result
    return highestAccounting;
  }

  private static void addWeight(Map<Account.Name, Integer> accountWeights, Account.Name account, int weight) {
    Integer i = accountWeights.get(account);
    int previous = i == null ? 0 : i;
    accountWeights.put(account, previous + weight);
  }

  public static boolean canAccountAccessHost(DatabaseConnection conn, Account.Name account, int host) throws IOException, SQLException {
    return conn.queryBoolean(
      "select\n"
      + "  (\n"
      + "    select\n"
      + "      id\n"
      + "    from\n"
      + "      account.\"AccountHost\"\n"
      + "    where\n"
      + "      accounting=?\n"
      + "      and server=?\n"
      + "    limit 1\n"
      + "  )\n"
      + "  is not null\n",
      account,
      host
    );
  }

  public static void checkAccountAccessHost(DatabaseConnection conn, RequestSource source, String action, Account.Name account, int host) throws IOException, SQLException {
    if (!canAccountAccessHost(conn, account, host)) {
      String message=
      "accounting="
      +account
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
