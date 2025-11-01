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

import com.aoapps.collections.IntArrayList;
import com.aoapps.collections.IntCollection;
import com.aoapps.collections.IntList;
import com.aoapps.collections.SortedArrayList;
import com.aoapps.dbc.DatabaseAccess;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.dns.DnsService;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * In the request lifecycle, table invalidations occur after the database connection has been committed
 * and released.  This ensures that all data is available for the processes that react to the table
 * updates.  For efficiency, each host and account will only be notified once per table per
 * request.
 *
 * @author  AO Industries, Inc.
 */
// TODO: This should use HashSet instead of SortedArrayList
public final class InvalidateList {

  /**
   * The invalidate list is used as part of the error logging, so it is not
   * logged to the ticket system.
   */
  private static final Logger logger = Logger.getLogger(InvalidateList.class.getName());

  /** Copy once to avoid repeated copies. */
  private static final Table.TableId[] tableIds = Table.TableId.values();
  // TODO: Unused 2018-11-18: private static final int numTables = tableIds.length;

  // TODO: Unused 2018-11-18: private static final String[] tableNames=new String[numTables];

  /**
   * Indicates that all hosts or account.Account should receive the invalidate signal.
   */
  public static final List<Account.Name> allAccounts = Collections.unmodifiableList(new ArrayList<>());
  public static final IntList allHosts = new IntArrayList();

  private final Map<Table.TableId, List<Integer>> hostLists = new EnumMap<>(Table.TableId.class);
  private final Map<Table.TableId, List<Account.Name>> accountLists = new EnumMap<>(Table.TableId.class);

  /**
   * Resets back to default state.
   */
  public void reset() {
    hostLists.clear();
    accountLists.clear();
  }

  public void addTable(
      DatabaseAccess db,
      Table.TableId tableId,
      Account.Name account,
      int host,
      boolean recurse
  ) throws IOException, SQLException {
    addTable(
        db,
        tableId,
        getAccountCollection(account),
        getHostCollection(host),
        recurse
    );
  }

  public void addTable(
      DatabaseAccess db,
      Table.TableId tableId,
      Collection<Account.Name> accounts,
      int host,
      boolean recurse
  ) throws IOException, SQLException {
    addTable(
        db,
        tableId,
        accounts,
        getHostCollection(host),
        recurse
    );
  }

  public void addTable(
      DatabaseAccess db,
      Table.TableId tableId,
      Account.Name account,
      IntCollection hosts,
      boolean recurse
  ) throws IOException, SQLException {
    addTable(
        db,
        tableId,
        getAccountCollection(account),
        hosts,
        recurse
    );
  }

  public void addTable(
      DatabaseAccess db,
      Table.TableId tableId,
      Collection<Account.Name> accounts,
      IntCollection hosts,
      boolean recurse
  ) throws IOException, SQLException {
    // TODO: Unused 2018-11-18:
    // if (tableNames[tableId.ordinal()] == null) {
    //   tableNames[tableId.ordinal()] = TableHandler.getTableName(conn, tableId);
    // }

    // Add to the account lists
    if (accounts == null || accounts == allAccounts) {
      accountLists.put(tableId, allAccounts);
    } else {
      if (!accounts.isEmpty()) {
        List<Account.Name> accountList = accountLists.get(tableId);
        // TODO: Just use HashSet here
        if (accountList == null) {
          accountLists.put(tableId, accountList = new SortedArrayList<>());
        }
        for (Account.Name account : accounts) {
          if (account == null) {
            logger.log(Level.WARNING, null, new RuntimeException("Warning: account is null"));
          } else if (!accountList.contains(account)) {
            accountList.add(account);
          }
        }
      }
    }

    // Add to the host lists
    if (hosts == null || hosts == allHosts) {
      hostLists.put(tableId, allHosts);
    } else if (!hosts.isEmpty()) {
      List<Integer> sv = hostLists.get(tableId);
      // TODO: Just use HashSet here
      if (sv == null) {
        hostLists.put(tableId, sv = new SortedArrayList<>());
      }
      for (Integer id : hosts) {
        if (id == null) {
          logger.log(Level.WARNING, null, new RuntimeException("Warning: id is null"));
        } else if (!sv.contains(id)) {
          sv.add(id);
        }
      }
    }

    // Recursively invalidate those tables who's filters might have been effected
    if (recurse) {
      switch (tableId) {
        case AO_SERVERS:
          addTable(db, Table.TableId.FIREWALLD_ZONES,       accounts, hosts, true);
          addTable(db, Table.TableId.LINUX_SERVER_ACCOUNTS, accounts, hosts, true);
          addTable(db, Table.TableId.LINUX_SERVER_GROUPS,   accounts, hosts, true);
          addTable(db, Table.TableId.MYSQL_SERVERS,         accounts, hosts, true);
          addTable(db, Table.TableId.POSTGRES_SERVERS,      accounts, hosts, true);
          break;
        case BUSINESS_SERVERS:
          addTable(db, Table.TableId.SERVERS, accounts, hosts, true);
          break;
        case BUSINESSES:
          addTable(db, Table.TableId.BUSINESS_PROFILES, accounts, hosts, true);
          break;
        case CYRUS_IMAPD_BINDS:
          addTable(db, Table.TableId.CYRUS_IMAPD_SERVERS, accounts, hosts, false);
          break;
        case CYRUS_IMAPD_SERVERS:
          addTable(db, Table.TableId.CYRUS_IMAPD_BINDS, accounts, hosts, false);
          break;
        case EMAIL_DOMAINS:
          addTable(db, Table.TableId.EMAIL_ADDRESSES,   accounts, hosts, true);
          addTable(db, Table.TableId.MAJORDOMO_SERVERS, accounts, hosts, true);
          break;
        case FAILOVER_FILE_REPLICATIONS:
          addTable(db, Table.TableId.SERVERS,      accounts, hosts, true);
          addTable(db, Table.TableId.NET_DEVICES,  accounts, hosts, true);
          addTable(db, Table.TableId.IP_ADDRESSES, accounts, hosts, true);
          addTable(db, Table.TableId.NET_BINDS,    accounts, hosts, true);
          break;
        case IP_REPUTATION_LIMITER_SETS:
          // Sets are only visible when used by at least one limiter in the same server farm
          addTable(db, Table.TableId.IP_REPUTATION_SETS,         accounts, hosts, true);
          addTable(db, Table.TableId.IP_REPUTATION_SET_HOSTS,    accounts, hosts, true);
          addTable(db, Table.TableId.IP_REPUTATION_SET_NETWORKS, accounts, hosts, true);
          break;
        case HTTPD_BINDS:
          addTable(db, Table.TableId.IP_ADDRESSES, accounts, hosts, true);
          addTable(db, Table.TableId.NET_BINDS,    accounts, hosts, false);
          break;
        case HTTPD_SITE_BINDS:
          addTable(db, Table.TableId.HTTPD_BINDS,             accounts, hosts, true);
          addTable(db, Table.TableId.HTTPD_SITE_BIND_HEADERS, accounts, hosts, false);
          addTable(db, Table.TableId.RewriteRule,             accounts, hosts, false);
          break;
        case HTTPD_TOMCAT_SITES:
          addTable(db, Table.TableId.HTTPD_TOMCAT_SITE_JK_MOUNTS, accounts, hosts, false);
          break;
        case IP_ADDRESSES:
          addTable(db, Table.TableId.IpAddressMonitoring, accounts, hosts, false);
          break;
        case LINUX_ACCOUNTS:
          addTable(db, Table.TableId.FTP_GUEST_USERS, accounts, hosts, true);
          addTable(db, Table.TableId.USERNAMES,       accounts, hosts, true);
          break;
        case LINUX_SERVER_ACCOUNTS:
          addTable(db, Table.TableId.LINUX_ACCOUNTS,       accounts, hosts, true);
          addTable(db, Table.TableId.LINUX_GROUP_ACCOUNTS, accounts, hosts, true);
          break;
        case LINUX_SERVER_GROUPS:
          addTable(db, Table.TableId.EMAIL_LISTS,          accounts, hosts, true);
          addTable(db, Table.TableId.LINUX_GROUPS,         accounts, hosts, true);
          addTable(db, Table.TableId.LINUX_GROUP_ACCOUNTS, accounts, hosts, true);
          break;
        case MAJORDOMO_SERVERS:
          addTable(db, Table.TableId.MAJORDOMO_LISTS, accounts, hosts, true);
          break;
        case MYSQL_SERVER_USERS:
          addTable(db, Table.TableId.MYSQL_USERS, accounts, hosts, true);
          break;
        case MYSQL_SERVERS:
          addTable(db, Table.TableId.NET_BINDS,          accounts, hosts, true);
          addTable(db, Table.TableId.MYSQL_DATABASES,    accounts, hosts, true);
          addTable(db, Table.TableId.MYSQL_SERVER_USERS, accounts, hosts, true);
          break;
        case NET_BINDS:
          addTable(db, Table.TableId.HTTPD_BINDS,              accounts, hosts, false);
          addTable(db, Table.TableId.NET_BIND_FIREWALLD_ZONES, accounts, hosts, false);
          break;
        case NET_BIND_FIREWALLD_ZONES:
          // Presence of "public" firewalld zone determines compatibility "open_firewall" for clients
          // version <= 1.80.2
          addTable(db, Table.TableId.NET_BINDS, accounts, hosts, false);
          break;
        case NET_DEVICES:
          addTable(db, Table.TableId.IP_ADDRESSES, accounts, hosts, true);
          break;
        case NOTICE_LOG:
          addTable(db, Table.TableId.NoticeLogBalance, accounts, hosts, false);
          break;
        case NoticeLogBalance:
          // Added for compatibility "balance" for pre-1.83.0 clients
          addTable(db, Table.TableId.NOTICE_LOG, accounts, hosts, false);
          break;
        case PACKAGE_DEFINITIONS:
          addTable(db, Table.TableId.PACKAGE_DEFINITION_LIMITS, accounts, hosts, true);
          break;
        case PACKAGES:
          addTable(db, Table.TableId.PACKAGE_DEFINITIONS, accounts, hosts, true);
          break;
        case POSTGRES_SERVER_USERS:
          addTable(db, Table.TableId.POSTGRES_USERS, accounts, hosts, true);
          break;
        case POSTGRES_SERVERS:
          addTable(db, Table.TableId.NET_BINDS,             accounts, hosts, true);
          addTable(db, Table.TableId.POSTGRES_DATABASES,    accounts, hosts, true);
          addTable(db, Table.TableId.POSTGRES_SERVER_USERS, accounts, hosts, true);
          break;
        case SENDMAIL_BINDS:
          addTable(db, Table.TableId.SENDMAIL_SERVERS, accounts, hosts, false);
          break;
        case SENDMAIL_SERVERS:
          addTable(db, Table.TableId.SENDMAIL_BINDS, accounts, hosts, false);
          break;
        case SERVERS:
          addTable(db, Table.TableId.AO_SERVERS,      accounts, hosts, true);
          addTable(db, Table.TableId.IP_ADDRESSES,    accounts, hosts, true);
          addTable(db, Table.TableId.NET_DEVICES,     accounts, hosts, true);
          addTable(db, Table.TableId.VIRTUAL_SERVERS, accounts, hosts, true);
          break;
        case SSL_CERTIFICATES:
          addTable(db, Table.TableId.SSL_CERTIFICATE_NAMES,      accounts, hosts, false);
          addTable(db, Table.TableId.SSL_CERTIFICATE_OTHER_USES, accounts, hosts, false);
          break;
        case USERNAMES:
          addTable(db, Table.TableId.BUSINESS_ADMINISTRATORS, accounts, hosts, true);
          break;
        case VIRTUAL_SERVERS:
          addTable(db, Table.TableId.VIRTUAL_DISKS, accounts, hosts, true);
          break;
        case WhoisHistoryAccount:
          addTable(db, Table.TableId.WhoisHistory, accounts, hosts, false);
          break;
        default:
          // fall-through
      }
    }
  }

  public List<Account.Name> getAffectedAccounts(Table.TableId tableId) {
    List<Account.Name> accountList = accountLists.get(tableId);
    if (accountList != null || hostLists.containsKey(tableId)) {
      return (accountList == null) ? allAccounts : accountList;
    } else {
      return null;
    }
  }

  public List<Integer> getAffectedHosts(Table.TableId tableId) {
    List<Integer> sv = hostLists.get(tableId);
    if (sv != null || accountLists.containsKey(tableId)) {
      return (sv == null) ? allHosts : sv;
    } else {
      return null;
    }
  }

  public void invalidateMasterCaches() {
    for (Table.TableId tableId : tableIds) {
      if (hostLists.containsKey(tableId) || accountLists.containsKey(tableId)) {
        AccountHandler.invalidateTable(tableId);
        CvsHandler.invalidateTable(tableId);
        DaemonHandler.invalidateTable(tableId);
        // TODO: Have each service register to receive invalidation signals
        try {
          AoservMaster.getService(DnsService.class).invalidateTable(tableId);
        } catch (NoServiceException e) {
          // OK when running batch credit card processing from command line
        }
        EmailHandler.invalidateTable(tableId);
        WebHandler.invalidateTable(tableId);
        LinuxAccountHandler.invalidateTable(tableId);
        AoservMaster.invalidateTable(tableId);
        MysqlHandler.invalidateTable(tableId);
        PackageHandler.invalidateTable(tableId);
        PostgresqlHandler.invalidateTable(tableId);
        NetHostHandler.invalidateTable(tableId);
        TableHandler.invalidateTable(tableId);
        AccountUserHandler.invalidateTable(tableId);
      }
    }
  }

  public boolean isInvalid(Table.TableId tableId) {
    return hostLists.containsKey(tableId) || accountLists.containsKey(tableId);
  }

  public static Collection<Account.Name> getAccountCollection(Account.Name... accounts) {
    if (accounts.length == 0) {
      return Collections.emptyList();
    }
    Collection<Account.Name> coll = new ArrayList<>(accounts.length);
    Collections.addAll(coll, accounts);
    return coll;
  }

  public static IntCollection getHostCollection(int... hosts) throws IOException, SQLException {
    if (hosts.length == 0) {
      return new IntArrayList(0);
    }
    IntCollection coll = new IntArrayList(hosts.length);
    for (int host : hosts) {
      coll.add(host);
    }
    return coll;
  }
}
