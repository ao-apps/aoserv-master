/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2017, 2018, 2019, 2020, 2021, 2022, 2024  AO Industries, Inc.
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
import com.aoapps.dbc.DatabaseAccess;
import com.aoapps.dbc.DatabaseAccess.Null;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.lang.i18n.Money;
import com.aoapps.lang.validation.ValidationException;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.billing.Package;
import com.aoindustries.aoserv.client.billing.Resource;
import com.aoindustries.aoserv.client.schema.Table;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The <code>PackageHandler</code> handles all the accesses to the <code>billing.Package</code> table.
 *
 * @author  AO Industries, Inc.
 */
public final class PackageHandler {

  /** Make no instances. */
  private PackageHandler() {
    throw new AssertionError();
  }

  private static final Map<Account.Name, Boolean> disabledPackages = new HashMap<>();

  public static boolean canPackageAccessHost(DatabaseConnection conn, RequestSource source, Account.Name packageName, int host) throws IOException, SQLException {
    return conn.queryBoolean(
        "select\n"
            + "  (\n"
            + "    select\n"
            + "      pk.id\n"
            + "    from\n"
            + "      billing.\"Package\" pk,\n"
            + "      account.\"AccountHost\" bs\n"
            + "    where\n"
            + "      pk.name=?\n"
            + "      and pk.accounting=bs.accounting\n"
            + "      and bs.server=?\n"
            + "    limit 1\n"
            + "  )\n"
            + "  is not null\n",
        packageName,
        host
    );
  }

  public static boolean canAccessPackage(DatabaseConnection conn, RequestSource source, Account.Name packageName) throws IOException, SQLException {
    return AccountHandler.canAccessAccount(conn, source, PackageHandler.getAccountForPackage(conn, packageName));
  }

  public static boolean canAccessPackage(DatabaseConnection conn, RequestSource source, int packageId) throws IOException, SQLException {
    return AccountHandler.canAccessAccount(conn, source, getAccountForPackage(conn, packageId));
  }

  public static boolean canAccessPackageDefinition(DatabaseConnection conn, RequestSource source, int packageDefinition) throws IOException, SQLException {
    return AccountHandler.canAccessAccount(conn, source, getAccountForPackageDefinition(conn, packageDefinition));
  }

  public static void checkAccessPackage(DatabaseConnection conn, RequestSource source, String action, Account.Name packageName) throws IOException, SQLException {
    if (!canAccessPackage(conn, source, packageName)) {
      String message =
          "currentAdministrator="
              + source.getCurrentAdministrator()
              + " is not allowed to access package: action='"
              + action
              + ", name="
              + packageName;
      throw new SQLException(message);
    }
  }

  public static void checkAccessPackage(DatabaseConnection conn, RequestSource source, String action, int packageId) throws IOException, SQLException {
    if (!canAccessPackage(conn, source, packageId)) {
      String message =
          "currentAdministrator="
              + source.getCurrentAdministrator()
              + " is not allowed to access package: action='"
              + action
              + ", id="
              + packageId;
      throw new SQLException(message);
    }
  }

  public static void checkAccessPackageDefinition(DatabaseConnection conn, RequestSource source, String action, int packageDefinition) throws IOException, SQLException {
    if (!canAccessPackageDefinition(conn, source, packageDefinition)) {
      String message =
          "currentAdministrator="
              + source.getCurrentAdministrator()
              + " is not allowed to access package: action='"
              + action
              + ", id="
              + packageDefinition;
      throw new SQLException(message);
    }
  }

  /**
   * Creates a new <code>Package</code>.
   */
  public static int addPackage(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      Account.Name packageName,
      Account.Name account,
      int packageDefinition
  ) throws IOException, SQLException {
    AccountHandler.checkAccessAccount(conn, source, "addPackage", account);
    if (AccountHandler.isAccountDisabled(conn, account)) {
      throw new SQLException("Unable to add Package '" + packageName + "', Account disabled: " + account);
    }

    // Check the PackageDefinition rules
    checkAccessPackageDefinition(conn, source, "addPackage", packageDefinition);
    // Businesses parent must be the package definition owner
    Account.Name parent = AccountHandler.getParentAccount(conn, account);
    Account.Name packageDefinitionBusiness = getAccountForPackageDefinition(conn, packageDefinition);
    if (!packageDefinitionBusiness.equals(parent)) {
      throw new SQLException("Unable to add Package '" + packageName + "', PackageDefinition #" + packageDefinition + " not owned by parent Account");
    }
    if (!isPackageDefinitionApproved(conn, packageDefinition)) {
      throw new SQLException("Unable to add Package '" + packageName + "', PackageDefinition not approved: " + packageDefinition);
    }
    //if (!isPackageDefinitionActive(conn, packageDefinition)) {
    //  throw new SQLException("Unable to add Package '"+packageName+"', PackageDefinition not active: "+packageDefinition);
    //}

    int packageId = conn.updateInt(
        "INSERT INTO\n"
            + "  billing.\"Package\"\n"
            + "VALUES (\n"
            + "  default,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  now(),\n"
            + "  ?,\n"
            + "  null,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?\n"
            + ") RETURNING id",
        packageName.toString(),
        account.toString(),
        packageDefinition,
        source.getCurrentAdministrator().toString(),
        Package.DEFAULT_EMAIL_IN_BURST,
        Package.DEFAULT_EMAIL_IN_RATE,
        Package.DEFAULT_EMAIL_OUT_BURST,
        Package.DEFAULT_EMAIL_OUT_RATE,
        Package.DEFAULT_EMAIL_RELAY_BURST,
        Package.DEFAULT_EMAIL_RELAY_RATE
    );

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.PACKAGES, account, InvalidateList.allHosts, false);

    return packageId;
  }

  /**
   * Creates a new <code>PackageDefinition</code>.
   */
  public static int addPackageDefinition(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      Account.Name account,
      String category,
      String name,
      String version,
      String display,
      String description,
      Money setupFee,
      String setupFeeTransactionType,
      Money monthlyRate,
      String monthlyRateTransactionType
  ) throws IOException, SQLException {
    AccountHandler.checkAccessAccount(conn, source, "addPackageDefinition", account);
    if (AccountHandler.isAccountDisabled(conn, account)) {
      throw new SQLException("Unable to add PackageDefinition, Account disabled: " + account);
    }

    int packageDefinition = conn.updateInt(
        "INSERT INTO\n"
            + "  billing.\"PackageDefinition\"\n"
            + "VALUES (\n"
            + "  default,\n" // id
            + "  ?,\n" // accounting
            + "  ?,\n" // category
            + "  ?,\n" // name
            + "  ?,\n" // version
            + "  ?,\n" // display
            + "  ?,\n" // description
            + "  ?,\n" // setupFee.currency
            + "  ?,\n" // setupFee.value
            + "  ?,\n" // setup_fee_transaction_type
            + "  ?,\n" // monthlyRate.currency
            + "  ?,\n" // monthlyRate.value
            + "  ?,\n" // monthly_rate_transaction_type
            + "  false,\n" // active
            + "  false\n" // approved
            + ") RETURNING id",
        account.toString(),
        category,
        name,
        version,
        display,
        description,
        setupFee == null ? null : setupFee.getCurrency().getCurrencyCode(),
        setupFee == null ? Null.NUMERIC : setupFee.getValue(),
        setupFeeTransactionType,
        monthlyRate.getCurrency().getCurrencyCode(),
        monthlyRate.getValue(),
        monthlyRateTransactionType
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.PACKAGE_DEFINITIONS,
        account,
        AccountHandler.getHostsForAccount(conn, account),
        false
    );

    return packageDefinition;
  }

  /**
   * Copies a <code>PackageDefinition</code>.
   */
  public static int copyPackageDefinition(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int packageDefinition
  ) throws IOException, SQLException {
    checkAccessPackageDefinition(conn, source, "copyPackageDefinition", packageDefinition);
    Account.Name account = getAccountForPackageDefinition(conn, packageDefinition);
    if (AccountHandler.isAccountDisabled(conn, account)) {
      throw new SQLException("Unable to copy PackageDefinition, Account disabled: " + account);
    }
    String category = conn.queryString("select category from billing.\"PackageDefinition\" where id=?", packageDefinition);
    String name = conn.queryString("select name from billing.\"PackageDefinition\" where id=?", packageDefinition);
    String version = conn.queryString("select version from billing.\"PackageDefinition\" where id=?", packageDefinition);
    String newVersion = null;
    for (int c = 1; c < Integer.MAX_VALUE; c++) {
      String temp = version + "." + c;
      if (
          conn.queryBoolean(
              "select (select id from billing.\"PackageDefinition\" where accounting=? and category=? and name=? and version=? limit 1) is null",
              account,
              category,
              name,
              temp
          )
      ) {
        newVersion = temp;
        break;
      }
    }
    if (newVersion == null) {
      throw new SQLException("Unable to generate new version for copy PackageDefinition: " + packageDefinition);
    }

    int newId = conn.updateInt(
        "INSERT INTO billing.\"PackageDefinition\" (\n"
            + "  accounting,\n"
            + "  category,\n"
            + "  name,\n"
            + "  version,\n"
            + "  display,\n"
            + "  description,\n"
            + "  \"setupFee.currency\",\n"
            + "  \"setupFee.value\",\n"
            + "  setup_fee_transaction_type,\n"
            + "  \"monthlyRate.currency\",\n"
            + "  \"monthlyRate.value\",\n"
            + "  monthly_rate_transaction_type\n"
            + ") SELECT\n"
            + "  accounting,\n"
            + "  category,\n"
            + "  name,\n"
            + "  ?,\n"
            + "  display,\n"
            + "  description,\n"
            + "  \"setupFee.currency\",\n"
            + "  \"setupFee.value\",\n"
            + "  setup_fee_transaction_type,\n"
            + "  \"monthlyRate.currency\",\n"
            + "  \"monthlyRate.value\",\n"
            + "  monthly_rate_transaction_type\n"
            + "FROM\n"
            + "  billing.\"PackageDefinition\"\n"
            + "WHERE\n"
            + "  id=?\n"
            + "RETURNING id",
        newVersion,
        packageDefinition
    );
    conn.update(
        "insert into\n"
            + "  billing.\"PackageDefinitionLimit\"\n"
            + "(\n"
            + "  package_definition,\n"
            + "  resource,\n"
            + "  soft_limit,\n"
            + "  hard_limit,\n"
            + "  \"additionalRate.currency\",\n"
            + "  \"additionalRate.value\",\n"
            + "  additional_transaction_type\n"
            + ") select\n"
            + "  ?,\n"
            + "  resource,\n"
            + "  soft_limit,\n"
            + "  hard_limit,\n"
            + "  \"additionalRate.currency\",\n"
            + "  \"additionalRate.value\",\n"
            + "  additional_transaction_type\n"
            + "from\n"
            + "  billing.\"PackageDefinitionLimit\"\n"
            + "where\n"
            + "  package_definition=?",
        newId,
        packageDefinition
    );

    // Notify all clients of the update
    IntList servers = AccountHandler.getHostsForAccount(conn, account);
    invalidateList.addTable(
        conn,
        Table.TableId.PACKAGE_DEFINITIONS,
        account,
        servers,
        false
    );
    invalidateList.addTable(
        conn,
        Table.TableId.PACKAGE_DEFINITION_LIMITS,
        account,
        servers,
        false
    );

    return newId;
  }

  public static void updatePackageDefinition(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int packageDefinition,
      Account.Name account,
      String category,
      String name,
      String version,
      String display,
      String description,
      Money setupFee,
      String setupFeeTransactionType,
      Money monthlyRate,
      String monthlyRateTransactionType
  ) throws IOException, SQLException {
    // Security checks
    checkAccessPackageDefinition(conn, source, "updatePackageDefinition", packageDefinition);
    AccountHandler.checkAccessAccount(conn, source, "updatePackageDefinition", account);
    if (isPackageDefinitionApproved(conn, packageDefinition)) {
      throw new SQLException("Not allowed to update an approved PackageDefinition: " + packageDefinition);
    }

    conn.update(
        "update\n"
            + "  billing.\"PackageDefinition\"\n"
            + "set\n"
            + "  accounting=?,\n"
            + "  category=?,\n"
            + "  name=?,\n"
            + "  version=?,\n"
            + "  display=?,\n"
            + "  description=?,\n"
            + "  \"setupFee.currency\"=?,\n"
            + "  \"setupFee.value\"=?,\n"
            + "  setup_fee_transaction_type=?,\n"
            + "  \"monthlyRate.currency\"=?,\n"
            + "  \"monthlyRate.value\"=?,\n"
            + "  monthly_rate_transaction_type=?\n"
            + "where\n"
            + "  id=?",
        account.toString(),
        category,
        name,
        version,
        display,
        description,
        setupFee == null ? null : setupFee.getCurrency().getCurrencyCode(),
        setupFee == null ? Null.NUMERIC : setupFee.getValue(),
        setupFeeTransactionType,
        monthlyRate.getCurrency().getCurrencyCode(),
        monthlyRate.getValue(),
        monthlyRateTransactionType,
        packageDefinition
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.PACKAGE_DEFINITIONS,
        account,
        AccountHandler.getHostsForAccount(conn, account),
        false
    );
  }

  public static void disablePackage(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int disableLog,
      Account.Name packageName
  ) throws IOException, SQLException {
    AccountHandler.checkAccessDisableLog(conn, source, "disablePackage", disableLog, false);
    checkAccessPackage(conn, source, "disablePackage", packageName);
    if (isPackageDisabled(conn, packageName)) {
      throw new SQLException("Package is already disabled: " + packageName);
    }
    IntList hsts = WebHandler.getHttpdSharedTomcatsForPackage(conn, packageName);
    for (int c = 0; c < hsts.size(); c++) {
      int hst = hsts.getInt(c);
      if (!WebHandler.isSharedTomcatDisabled(conn, hst)) {
        throw new SQLException("Cannot disable Package '" + packageName + "': SharedTomcat not disabled: " + hst);
      }
    }
    IntList eps = EmailHandler.getPipesForPackage(conn, packageName);
    for (int c = 0; c < eps.size(); c++) {
      int ep = eps.getInt(c);
      if (!EmailHandler.isPipeDisabled(conn, ep)) {
        throw new SQLException("Cannot disable Package '" + packageName + "': Pipe not disabled: " + ep);
      }
    }
    List<com.aoindustries.aoserv.client.account.User.Name> users = AccountUserHandler.getUsersForPackage(conn, packageName);
    for (com.aoindustries.aoserv.client.account.User.Name user : users) {
      if (!AccountUserHandler.isUserDisabled(conn, user)) {
        throw new SQLException("Cannot disable Package '" + packageName + "': Username not disabled: " + user);
      }
    }
    IntList hss = WebHandler.getHttpdSitesForPackage(conn, packageName);
    for (int c = 0; c < hss.size(); c++) {
      int hs = hss.getInt(c);
      if (!WebHandler.isSiteDisabled(conn, hs)) {
        throw new SQLException("Cannot disable Package '" + packageName + "': Site not disabled: " + hs);
      }
    }
    IntList els = EmailHandler.getListsForPackage(conn, packageName);
    for (int c = 0; c < els.size(); c++) {
      int el = els.getInt(c);
      if (!EmailHandler.isListDisabled(conn, el)) {
        throw new SQLException("Cannot disable Package '" + packageName + "': List not disabled: " + el);
      }
    }
    IntList ssrs = EmailHandler.getSmtpRelaysForPackage(conn, packageName);
    for (int c = 0; c < ssrs.size(); c++) {
      int ssr = ssrs.getInt(c);
      if (!EmailHandler.isSmtpRelayDisabled(conn, ssr)) {
        throw new SQLException("Cannot disable Package '" + packageName + "': SmtpRelay not disabled: " + ssr);
      }
    }

    conn.update(
        "update billing.\"Package\" set disable_log=? where name=?",
        disableLog,
        packageName
    );

    // Notify all clients of the update
    Account.Name account = PackageHandler.getAccountForPackage(conn, packageName);
    invalidateList.addTable(
        conn,
        Table.TableId.PACKAGES,
        account,
        AccountHandler.getHostsForAccount(conn, account),
        false
    );
  }

  public static void enablePackage(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      Account.Name packageName
  ) throws IOException, SQLException {
    checkAccessPackage(conn, source, "enablePackage", packageName);
    int disableLog = getDisableLogForPackage(conn, packageName);
    if (disableLog == -1) {
      throw new SQLException("Package is already enabled: " + packageName);
    }
    AccountHandler.checkAccessDisableLog(conn, source, "enablePackage", disableLog, true);
    Account.Name account = PackageHandler.getAccountForPackage(conn, packageName);
    if (AccountHandler.isAccountDisabled(conn, account)) {
      throw new SQLException("Unable to enable Package '" + packageName + "', Account not enabled: " + account);
    }

    conn.update(
        "update billing.\"Package\" set disable_log=null where name=?",
        packageName
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.PACKAGES,
        account,
        AccountHandler.getHostsForAccount(conn, account),
        false
    );
  }

  public static Account.Name generatePackageName(
      DatabaseConnection conn,
      Account.Name template
  ) throws IOException, SQLException {
    // Load the entire list of package names
    Set<Account.Name> names = conn.queryNewCollection(
        AoCollections::newHashSet,
        ObjectFactories.accountNameFactory,
        "select name from billing.\"Package\""
    );
    // Find one that is not used
    for (int c = 0; c < Integer.MAX_VALUE; c++) {
      Account.Name name;
      try {
        name = Account.Name.valueOf(template.toString() + c);
      } catch (ValidationException e) {
        throw new SQLException(e);
      }
      if (!names.contains(name)) {
        return name;
      }
    }
    // If could not find one, report and error
    throw new SQLException("Unable to find available package name for template: " + template);
  }

  public static int getDisableLogForPackage(DatabaseConnection conn, Account.Name packageName) throws IOException, SQLException {
    return conn.queryInt("select coalesce(disable_log, -1) from billing.\"Package\" where name=?", packageName);
  }

  public static void invalidateTable(Table.TableId tableId) {
    if (tableId == Table.TableId.PACKAGES) {
      synchronized (PackageHandler.class) {
        disabledPackages.clear();
      }
      synchronized (packageAccounts) {
        packageAccounts.clear();
      }
      synchronized (packageNames) {
        packageNames.clear();
      }
      synchronized (packageIds) {
        packageIds.clear();
      }
    }
  }

  public static boolean isPackageDisabled(DatabaseConnection conn, Account.Name packageName) throws IOException, SQLException {
    synchronized (PackageHandler.class) {
      Boolean o = disabledPackages.get(packageName);
      if (o != null) {
        return o;
      }
      boolean isDisabled = getDisableLogForPackage(conn, packageName) != -1;
      disabledPackages.put(packageName, isDisabled);
      return isDisabled;
    }
  }

  public static boolean isPackageNameAvailable(DatabaseConnection conn, Account.Name packageName) throws IOException, SQLException {
    return conn.queryBoolean("select (select id from billing.\"Package\" where name=? limit 1) is null", packageName);
  }

  /**
   * Used for compatibility with older clients.
   */
  public static int findActivePackageDefinition(DatabaseConnection conn, Account.Name account, Money rate, int userLimit, int popLimit) throws IOException, SQLException {
    return conn.queryInt(
        "select\n"
            + "  coalesce(\n"
            + "    (\n"
            + "      select\n"
            + "        pd.id\n"
            + "      from\n"
            + "        billing.\"PackageDefinition\" pd,\n"
            + "        package_definitions_limits user_pdl,\n"
            + "        package_definitions_limits pop_pdl\n"
            + "      where\n"
            + "        pd.accounting=?\n"
            + "        and pd.\"monthlyRate.currency\"=?\n"
            + "        and pd.\"monthlyRate.value\"=?\n"
            + "        and pd.id=user_pdl.package_definition\n"
            + "        and user_pdl.resource=?\n"
            + "        and pd.id=pop_pdl.package_definition\n"
            + "        and pop_pdl.resource=?\n"
            + "      limit 1\n"
            + "    ), -1\n"
            + "  )",
        account,
        rate.getCurrency().getCurrencyCode(),
        rate.getValue(),
        Resource.USER,
        Resource.EMAIL
    );
  }

  public static boolean isPackageDefinitionApproved(DatabaseConnection conn, int packageDefinition) throws IOException, SQLException {
    return conn.queryBoolean("select approved from billing.\"PackageDefinition\" where id=?", packageDefinition);
  }

  public static boolean isPackageDefinitionActive(DatabaseConnection conn, int packageDefinition) throws IOException, SQLException {
    return conn.queryBoolean("select active from billing.\"PackageDefinition\" where id=?", packageDefinition);
  }

  public static void checkPackageAccessHost(DatabaseConnection conn, RequestSource source, String action, Account.Name packageName, int host) throws IOException, SQLException {
    if (!canPackageAccessHost(conn, source, packageName, host)) {
      String message =
          "package.name="
              + packageName
              + " is not allowed to access server.id="
              + host
              + ": action='"
              + action
              + "'";
      throw new SQLException(message);
    }
  }

  private static final Map<Integer, Account.Name> packageAccounts = new HashMap<>();

  public static Account.Name getAccountForPackage(DatabaseAccess database, Account.Name packageName) throws IOException, SQLException {
    return getAccountForPackage(database, getIdForPackage(database, packageName));
  }

  public static Account.Name getAccountForPackage(DatabaseAccess database, int packageId) throws IOException, SQLException {
    Integer i = packageId;
    synchronized (packageAccounts) {
      Account.Name o = packageAccounts.get(i);
      if (o != null) {
        return o;
      }
      Account.Name business = database.queryObject(
          ObjectFactories.accountNameFactory,
          "select accounting from billing.\"Package\" where id=?",
          packageId
      );
      packageAccounts.put(i, business);
      return business;
    }
  }

  private static final Map<Integer, Account.Name> packageNames = new HashMap<>();

  public static Account.Name getNameForPackage(DatabaseConnection conn, int packageId) throws IOException, SQLException {
    Integer i = packageId;
    synchronized (packageNames) {
      Account.Name o = packageNames.get(i);
      if (o != null) {
        return o;
      }
      Account.Name name = conn.queryObject(
          ObjectFactories.accountNameFactory,
          "select name from billing.\"Package\" where id=?",
          packageId
      );
      packageNames.put(i, name);
      return name;
    }
  }

  private static final Map<Account.Name, Integer> packageIds = new HashMap<>();

  public static int getIdForPackage(DatabaseAccess database, Account.Name name) throws IOException, SQLException {
    synchronized (packageIds) {
      Integer o = packageIds.get(name);
      if (o != null) {
        return o;
      }
      int packageId = database.queryInt("select id from billing.\"Package\" where name=?", name);
      packageIds.put(name, packageId);
      return packageId;
    }
  }

  public static Account.Name getAccountForPackageDefinition(DatabaseConnection conn, int packageDefinition) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select accounting from billing.\"PackageDefinition\" where id=?",
        packageDefinition
    );
  }

  public static List<Account.Name> getAccountsForPackageDefinition(DatabaseConnection conn, int packageDefinition) throws IOException, SQLException {
    return conn.queryList(
        ObjectFactories.accountNameFactory,
        "select distinct\n"
            + "  bu.accounting\n"
            + "from\n"
            + "  billing.\"Package\" pk,\n"
            + "  account.\"Account\" bu\n"
            + "where\n"
            + "  pk.package_definition=?\n"
            + "  and pk.accounting=bu.accounting",
        packageDefinition
    );
  }

  public static void setPackageDefinitionActive(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int packageDefinition,
      boolean isActive
  ) throws IOException, SQLException {
    checkAccessPackageDefinition(conn, source, "setPackageDefinitionActive", packageDefinition);
    // Must be approved to be activated
    if (isActive && !isPackageDefinitionApproved(conn, packageDefinition)) {
      throw new SQLException("PackageDefinition must be approved before it may be activated: " + packageDefinition);
    }

    // Update the database
    conn.update(
        "update billing.\"PackageDefinition\" set active=? where id=?",
        isActive,
        packageDefinition
    );

    invalidateList.addTable(conn,
        Table.TableId.PACKAGE_DEFINITIONS,
        getAccountForPackageDefinition(conn, packageDefinition),
        InvalidateList.allHosts,
        false
    );
    invalidateList.addTable(conn,
        Table.TableId.PACKAGE_DEFINITIONS,
        getAccountsForPackageDefinition(conn, packageDefinition),
        InvalidateList.allHosts,
        false
    );
  }

  public static void setPackageDefinitionLimits(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int packageDefinition,
      String[] resources,
      int[] softLimits,
      int[] hardLimits,
      Money[] additionalRates,
      String[] additionalTransactionTypes
  ) throws IOException, SQLException {
    checkAccessPackageDefinition(conn, source, "setPackageDefinitionLimits", packageDefinition);
    // Must not be approved to be edited
    if (isPackageDefinitionApproved(conn, packageDefinition)) {
      throw new SQLException("PackageDefinition may not have its limits set after it is approved: " + packageDefinition);
    }

    // Update the database
    conn.update("delete from billing.\"PackageDefinitionLimit\" where package_definition=?", packageDefinition);
    for (int c = 0; c < resources.length; c++) {
      Money additionalRate = additionalRates[c];
      conn.update(
          "insert into\n"
              + "  billing.\"PackageDefinitionLimit\"\n"
              + "(\n"
              + "  package_definition,\n"
              + "  resource,\n"
              + "  soft_limit,\n"
              + "  hard_limit,\n"
              + "  \"additionalRate.currency\",\n"
              + "  \"additionalRate.value\",\n"
              + "  additional_transaction_type\n"
              + ") values(\n"
              + "  ?,\n"
              + "  ?,\n"
              + "  ?,\n"
              + "  ?,\n"
              + "  ?,\n"
              + "  ?,\n"
              + "  ?\n"
              + ")",
          packageDefinition,
          resources[c],
          softLimits[c] == -1 ? Null.INTEGER : softLimits[c],
          hardLimits[c] == -1 ? Null.INTEGER : hardLimits[c],
          additionalRate == null ? null : additionalRate.getCurrency().getCurrencyCode(),
          additionalRate == null ? Null.NUMERIC : additionalRate.getValue(),
          additionalTransactionTypes[c]
      );
    }

    invalidateList.addTable(conn,
        Table.TableId.PACKAGE_DEFINITION_LIMITS,
        getAccountForPackageDefinition(conn, packageDefinition),
        InvalidateList.allHosts,
        false
    );
    invalidateList.addTable(conn,
        Table.TableId.PACKAGE_DEFINITION_LIMITS,
        getAccountsForPackageDefinition(conn, packageDefinition),
        InvalidateList.allHosts,
        false
    );
  }

  public static void removePackageDefinition(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int packageDefinition
  ) throws IOException, SQLException {
    // Security checks
    PackageHandler.checkAccessPackageDefinition(conn, source, "removePackageDefinition", packageDefinition);

    // Do the remove
    removePackageDefinition(conn, invalidateList, packageDefinition);
  }

  public static void removePackageDefinition(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      int packageDefinition
  ) throws IOException, SQLException {
    Account.Name account = getAccountForPackageDefinition(conn, packageDefinition);
    IntList servers = AccountHandler.getHostsForAccount(conn, account);
    if (conn.update("delete from billing.\"PackageDefinitionLimit\" where package_definition=?", packageDefinition) > 0) {
      invalidateList.addTable(
          conn,
          Table.TableId.PACKAGE_DEFINITION_LIMITS,
          account,
          servers,
          false
      );
    }

    conn.update("delete from billing.\"PackageDefinition\" where id=?", packageDefinition);
    invalidateList.addTable(
        conn,
        Table.TableId.PACKAGE_DEFINITIONS,
        account,
        servers,
        false
    );
  }
}
