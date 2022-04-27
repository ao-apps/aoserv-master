/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022  AO Industries, Inc.
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
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableInput;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.AOServWritable;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.account.AccountHost;
import com.aoindustries.aoserv.client.account.Administrator;
import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.net.Host;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.AbstractList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The <code>TableHandler</code> handles all the accesses to the AOServ tables.
 *
 * @author  AO Industries, Inc.
 */
public final class TableHandler {

  /** Make no instances. */
  private TableHandler() {
    throw new AssertionError();
  }

  private static final Logger logger = Logger.getLogger(TableHandler.class.getName());

  private static boolean started;

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static void start() {
    synchronized (System.out) {
      if (!started) {
        System.out.print("Starting " + TableHandler.class.getSimpleName());
        initGetObjectHandlers(System.out);
        initGetTableHandlers(System.out);
        started = true;
        System.out.println(": Done");
      }
    }
  }

  /**
   * The number of rows statements that should typically be used per update/insert/delete batch.
   */
  public static final int UPDATE_BATCH_SIZE = 1000;

  /**
   * The number of updates that will typically be done before the changes are committed.
   */
  public static final int BATCH_COMMIT_INTERVAL = 1000;

  /*
   * TODO: Use WITH RECURSIVE and no longer limit business tree depth.
   *
   * Example query to get an account and all its parents:
   *
   * WITH RECURSIVE account_and_up(accounting) AS (
   *   VALUES ('LOG_NEWRANKS_NET')
   * UNION ALL
   *   SELECT a.parent FROM
   *     account_and_up
   *     INNER JOIN account."Account" a ON account_and_up.accounting = a.accounting
   *   WHERE
   *     a.parent IS NOT NULL
   * )
   * SELECT * FROM account_and_up;
   *
   * Example query to get an account and all its subaccounts:
   *
   * WITH RECURSIVE account_and_down(accounting) AS (
   *   VALUES ('WOOT_WHMCS')
   * UNION ALL
   *   SELECT a.accounting FROM
   *     account_and_down
   *     INNER JOIN account."Account" a ON account_and_down.accounting = a.parent
   * )
   * SELECT * FROM account_and_down;
   */

  /**
   * The joins used for the business tree.
   */
  public static final String
      BU1_PARENTS_JOIN =
          "  account.\"Account\" bu1\n"
              + "  left join account.\"Account\" bu2 on bu1.parent=bu2.accounting\n"
              + "  left join account.\"Account\" bu3 on bu2.parent=bu3.accounting\n"
              + "  left join account.\"Account\" bu4 on bu3.parent=bu4.accounting\n"
              + "  left join account.\"Account\" bu5 on bu4.parent=bu5.accounting\n"
              + "  left join account.\"Account\" bu" + (Account.MAXIMUM_BUSINESS_TREE_DEPTH - 1) + " on bu5.parent=bu" + (Account.MAXIMUM_BUSINESS_TREE_DEPTH - 1) + ".accounting,\n",
      BU1_PARENTS_JOIN_NO_COMMA =
          "  account.\"Account\" bu1\n"
              + "  left join account.\"Account\" bu2 on bu1.parent=bu2.accounting\n"
              + "  left join account.\"Account\" bu3 on bu2.parent=bu3.accounting\n"
              + "  left join account.\"Account\" bu4 on bu3.parent=bu4.accounting\n"
              + "  left join account.\"Account\" bu5 on bu4.parent=bu5.accounting\n"
              + "  left join account.\"Account\" bu" + (Account.MAXIMUM_BUSINESS_TREE_DEPTH - 1) + " on bu5.parent=bu" + (Account.MAXIMUM_BUSINESS_TREE_DEPTH - 1) + ".accounting\n",
      BU2_PARENTS_JOIN =
          "      account.\"Account\" bu" + Account.MAXIMUM_BUSINESS_TREE_DEPTH + "\n"
              + "      left join account.\"Account\" bu8 on bu7.parent=bu8.accounting\n"
              + "      left join account.\"Account\" bu9 on bu8.parent=bu9.accounting\n"
              + "      left join account.\"Account\" bu10 on bu9.parent=bu10.accounting\n"
              + "      left join account.\"Account\" bu11 on bu10.parent=bu11.accounting\n"
              + "      left join account.\"Account\" bu" + (Account.MAXIMUM_BUSINESS_TREE_DEPTH * 2 - 2) + " on bu11.parent=bu" + (Account.MAXIMUM_BUSINESS_TREE_DEPTH * 2 - 2) + ".accounting,\n"
  ;

  /**
   * The where clauses that accompany the joins.
   */
  public static final String
      PK_BU1_PARENTS_WHERE =
          "    pk.accounting=bu1.accounting\n"
              + "    or pk.accounting=bu1.parent\n"
              + "    or pk.accounting=bu2.parent\n"
              + "    or pk.accounting=bu3.parent\n"
              + "    or pk.accounting=bu4.parent\n"
              + "    or pk.accounting=bu5.parent\n"
              + "    or pk.accounting=bu" + (Account.MAXIMUM_BUSINESS_TREE_DEPTH - 1) + ".parent\n",
      PK1_BU1_PARENTS_OR_WHERE =
          "    or pk1.accounting=bu1.accounting\n"
              + "    or pk1.accounting=bu1.parent\n"
              + "    or pk1.accounting=bu2.parent\n"
              + "    or pk1.accounting=bu3.parent\n"
              + "    or pk1.accounting=bu4.parent\n"
              + "    or pk1.accounting=bu5.parent\n"
              + "    or pk1.accounting=bu" + (Account.MAXIMUM_BUSINESS_TREE_DEPTH - 1) + ".parent\n",
      PK1_BU1_PARENTS_WHERE =
          "    pk1.accounting=bu1.accounting\n"
              + "    or pk1.accounting=bu1.parent\n"
              + "    or pk1.accounting=bu2.parent\n"
              + "    or pk1.accounting=bu3.parent\n"
              + "    or pk1.accounting=bu4.parent\n"
              + "    or pk1.accounting=bu5.parent\n"
              + "    or pk1.accounting=bu" + (Account.MAXIMUM_BUSINESS_TREE_DEPTH - 1) + ".parent\n",
      PK3_BU2_PARENTS_OR_WHERE =
          "        or pk3.accounting=bu" + Account.MAXIMUM_BUSINESS_TREE_DEPTH + ".accounting\n"
              + "        or pk3.accounting=bu" + Account.MAXIMUM_BUSINESS_TREE_DEPTH + ".parent\n"
              + "        or pk3.accounting=bu8.parent\n"
              + "        or pk3.accounting=bu9.parent\n"
              + "        or pk3.accounting=bu10.parent\n"
              + "        or pk3.accounting=bu11.parent\n"
              + "        or pk3.accounting=bu" + (Account.MAXIMUM_BUSINESS_TREE_DEPTH * 2 - 2) + ".parent\n",
      PK3_BU2_PARENTS_WHERE =
          "        pk3.accounting=bu" + Account.MAXIMUM_BUSINESS_TREE_DEPTH + ".accounting\n"
              + "        or pk3.accounting=bu" + Account.MAXIMUM_BUSINESS_TREE_DEPTH + ".parent\n"
              + "        or pk3.accounting=bu8.parent\n"
              + "        or pk3.accounting=bu9.parent\n"
              + "        or pk3.accounting=bu10.parent\n"
              + "        or pk3.accounting=bu11.parent\n"
              + "        or pk3.accounting=bu" + (Account.MAXIMUM_BUSINESS_TREE_DEPTH * 2 - 2) + ".parent\n"
  ;

  public static interface GetObjectHandler {
    /**
     * Gets the set of tables handled.
     */
    java.util.Set<Table.TableID> getTableIds();

    /**
     * Handles a client request for the given table.
     */
    void getObject(
        DatabaseConnection conn,
        RequestSource source,
        StreamableInput in,
        StreamableOutput out,
        Table.TableID tableID,
        User masterUser,
        UserHost[] masterServers
    ) throws IOException, SQLException;
  }

  private static final ConcurrentMap<Table.TableID, GetObjectHandler> getObjectHandlers = new ConcurrentHashMap<>();

  /**
   * This is available, but recommend registering via {@link ServiceLoader}.
   */
  public static int addGetObjectHandler(GetObjectHandler handler) {
    int numTables = 0;
    {
      boolean successful = false;
      java.util.Set<Table.TableID> added = EnumSet.noneOf(Table.TableID.class);
      try {
        for (Table.TableID tableID : handler.getTableIds()) {
          GetObjectHandler existing = getObjectHandlers.putIfAbsent(tableID, handler);
          if (existing != null) {
            throw new IllegalStateException("Handler already registered for table " + tableID + ": " + existing);
          }
          added.add(tableID);
          numTables++;
        }
        successful = true;
      } finally {
        if (!successful) {
          // Rollback partial
          for (Table.TableID id : added) {
            getObjectHandlers.remove(id);
          }
        }
      }
    }
    if (numTables == 0 && logger.isLoggable(Level.WARNING)) {
      logger.log(Level.WARNING, GetObjectHandler.class.getSimpleName() + " did not specify any tables: " + handler);
    }
    return numTables;
  }

  private static void printCounts(
      PrintStream out,
      Class<?> type,
      int handlerCount,
      int tableCount
  ) {
    out.print(type.getSimpleName());
    out.print(" (");
    out.print(handlerCount);
    out.print(' ');
    out.print(handlerCount == 1 ? "handler" : "handlers");
    out.print(" for ");
    out.print(tableCount);
    out.print(' ');
    out.print(tableCount == 1 ? "table" : "tables");
    out.print(')');
  }

  static void initGetObjectHandlers(Iterator<GetObjectHandler> handlers, PrintStream out, boolean hideIfZero) {
    int tableCount = 0;
    int handlerCount = 0;
    while (handlers.hasNext()) {
      tableCount += addGetObjectHandler(handlers.next());
      handlerCount++;
    }
    if (!hideIfZero || handlerCount != 0) {
      out.print(": ");
      printCounts(
          out,
          GetObjectHandler.class,
          handlerCount,
          tableCount
      );
    }
  }

  private static void initGetObjectHandlers(PrintStream out) {
    initGetObjectHandlers(ServiceLoader.load(GetObjectHandler.class).iterator(), out, false);
  }

  /**
   * Gets one object from a table.
   */
  public static void getObject(
      DatabaseConnection conn,
      RequestSource source,
      StreamableInput in,
      StreamableOutput out,
      Table.TableID tableID
  ) throws IOException, SQLException {
    com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
    User masterUser = MasterServer.getUser(conn, currentAdministrator);
    UserHost[] masterServers = masterUser == null ? null : MasterServer.getUserHosts(conn, currentAdministrator);

    GetObjectHandler handler = getObjectHandlers.get(tableID);
    if (handler != null) {
      // TODO: release conn before writing to out
      handler.getObject(conn, source, in, out, tableID, masterUser, masterServers);
    } else {
      throw new IOException("No " + GetObjectHandler.class.getSimpleName() + " registered for table ID: " + tableID);
    }
  }

  /**
   * Caches row counts for each table on a per-username basis.
   */
  private static final Map<com.aoindustries.aoserv.client.account.User.Name, int[]> rowCountsPerUsername = new HashMap<>();
  private static final Map<com.aoindustries.aoserv.client.account.User.Name, long[]> expireTimesPerUsername = new HashMap<>();

  static final int MAX_ROW_COUNT_CACHE_AGE = 60 * 60 * 1000; // One hour

  /** Copy used to avoid multiple array copies on each access. */
  private static final Table.TableID[] _tableIDs = Table.TableID.values();
  private static final int _numTables = _tableIDs.length;

  public static int getCachedRowCount(
      DatabaseConnection conn,
      RequestSource source,
      Table.TableID tableID
  ) throws IOException, SQLException {
    com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();

    // Synchronize to get the correct objects
    int[] rowCounts;
    long[] expireTimes;
    synchronized (rowCountsPerUsername) {
      rowCounts = rowCountsPerUsername.get(currentAdministrator);
      if (rowCounts == null) {
        rowCountsPerUsername.put(currentAdministrator, rowCounts = new int[_numTables]);
        expireTimesPerUsername.put(currentAdministrator, expireTimes = new long[_numTables]);
      } else {
        expireTimes = expireTimesPerUsername.get(currentAdministrator);
      }
    }

    // Synchronize on the array to provide a per-user lock
    synchronized (rowCounts) {
      long expireTime = expireTimes[tableID.ordinal()];
      long startTime = System.currentTimeMillis();
      if (
          expireTime == 0
              || expireTime <= startTime
              || expireTime > (startTime + MAX_ROW_COUNT_CACHE_AGE)
      ) {
        rowCounts[tableID.ordinal()] = getRowCount(
            conn,
            source,
            tableID
        );
        expireTimes[tableID.ordinal()] = System.currentTimeMillis() + MAX_ROW_COUNT_CACHE_AGE;
      }

      return rowCounts[tableID.ordinal()];
    }
  }

  /**
   * Gets the number of accessible rows in a table.
   */
  public static int getRowCount(
      DatabaseConnection conn,
      RequestSource source,
      Table.TableID tableID
  ) throws IOException, SQLException {
    com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
    User masterUser = MasterServer.getUser(conn, currentAdministrator);
    UserHost[] masterServers = masterUser == null ? null : MasterServer.getUserHosts(conn, currentAdministrator);
    switch (tableID) {
      case DISTRO_FILES :
        if (masterUser != null) {
          assert masterServers != null;
          if (masterServers.length == 0) {
            if (source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_107) <= 0) {
              return 0;
            } else {
              return conn.queryInt(
                  "select count(*) from \"distribution.management\".\"DistroFile\""
              );
            }
          } else {
            // Restrict to the operating system versions accessible to this user
            IntList osVersions = getOperatingSystemVersions(conn, source);
            if (osVersions.isEmpty()) {
              return 0;
            }
            StringBuilder sql = new StringBuilder();
            sql.append("select count(*) from \"distribution.management\".\"DistroFile\" where operating_system_version in (");
            for (int c = 0; c < osVersions.size(); c++) {
              if (c > 0) {
                sql.append(',');
              }
              sql.append(osVersions.getInt(c));
            }
            sql.append(')');
            return conn.queryInt(sql.toString());
          }
        } else {
          return 0;
        }
      case TICKETS :
        if (source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_43) <= 0) {
          // For backwards-compatibility only
          return 0;
        }
        throw new IOException("Unknown table ID: " + tableID); // No longer used as of version 1.44
      default :
        throw new IOException("Unknown table ID: " + tableID);
    }
  }

  public static interface GetTableHandler {
    /**
     * Gets the set of tables handled.
     */
    java.util.Set<Table.TableID> getTableIds();

    /**
     * Handles a client request for the given table.
     */
    void getTable(
        DatabaseConnection conn,
        RequestSource source,
        StreamableOutput out,
        boolean provideProgress,
        Table.TableID tableID,
        User masterUser,
        UserHost[] masterServers
    ) throws IOException, SQLException;
  }

  private static final ConcurrentMap<Table.TableID, GetTableHandler> getTableHandlers = new ConcurrentHashMap<>();

  /**
   * This is available, but recommend registering via {@link ServiceLoader}.
   */
  public static int addGetTableHandler(GetTableHandler handler) {
    int numTables = 0;
    {
      boolean successful = false;
      java.util.Set<Table.TableID> added = EnumSet.noneOf(Table.TableID.class);
      try {
        for (Table.TableID tableID : handler.getTableIds()) {
          GetTableHandler existing = getTableHandlers.putIfAbsent(tableID, handler);
          if (existing != null) {
            throw new IllegalStateException("Handler already registered for table " + tableID + ": " + existing);
          }
          added.add(tableID);
          numTables++;
        }
        successful = true;
      } finally {
        if (!successful) {
          // Rollback partial
          for (Table.TableID id : added) {
            getTableHandlers.remove(id);
          }
        }
      }
    }
    if (numTables == 0 && logger.isLoggable(Level.WARNING)) {
      logger.log(Level.WARNING, GetTableHandler.class.getSimpleName() + " did not specify any tables: " + handler);
    }
    return numTables;
  }

  static void initGetTableHandlers(Iterator<GetTableHandler> handlers, PrintStream out, boolean hideIfZero) {
    int tableCount = 0;
    int handlerCount = 0;
    while (handlers.hasNext()) {
      tableCount += addGetTableHandler(handlers.next());
      handlerCount++;
    }
    if (!hideIfZero || handlerCount != 0) {
      out.print(": ");
      printCounts(
          out,
          GetTableHandler.class,
          handlerCount,
          tableCount
      );
    }
  }

  private static void initGetTableHandlers(PrintStream out) {
    initGetTableHandlers(ServiceLoader.load(GetTableHandler.class).iterator(), out, false);
  }

  public abstract static class GetTableHandlerByRole implements GetTableHandler {

    /**
     * Calls role-specific implementations.
     */
    @Override
    public void getTable(
        DatabaseConnection conn,
        RequestSource source,
        StreamableOutput out,
        boolean provideProgress,
        Table.TableID tableID,
        User masterUser,
        UserHost[] masterServers
    ) throws IOException, SQLException {
      if (masterUser != null) {
        assert masterServers != null;
        if (masterServers.length == 0) {
          getTableMaster(conn, source, out, provideProgress, tableID, masterUser);
        } else {
          getTableDaemon(conn, source, out, provideProgress, tableID, masterUser, masterServers);
        }
      } else {
        getTableAdministrator(conn, source, out, provideProgress, tableID);
      }
    }

    /**
     * Handles a {@link User master user} request for the given table, with
     * access to all {@link Account accounts} and {@link Host hosts}.
     */
    protected abstract void getTableMaster(
        DatabaseConnection conn,
        RequestSource source,
        StreamableOutput out,
        boolean provideProgress,
        Table.TableID tableID,
        User masterUser
    ) throws IOException, SQLException;

    /**
     * Handles a {@link User master user} request for the given table, with
     * access limited to a set of {@link Host hosts}.  This is the filtering
     * generally used by <a href="https://aoindustries.com/aoserv/daemon/">AOServ Daemon</a>.
     */
    protected abstract void getTableDaemon(
        DatabaseConnection conn,
        RequestSource source,
        StreamableOutput out,
        boolean provideProgress,
        Table.TableID tableID,
        User masterUser,
        UserHost[] masterServers
    ) throws IOException, SQLException;

    /**
     * Handles an {@link Administrator} request for the given table, with
     * access limited by their set of {@link Account accounts} and the
     * {@link Host hosts} those accounts can access (see {@link AccountHost}.
     */
    protected abstract void getTableAdministrator(
        DatabaseConnection conn,
        RequestSource source,
        StreamableOutput out,
        boolean provideProgress,
        Table.TableID tableID
    ) throws IOException, SQLException;
  }

  public abstract static class GetTableHandlerPublic implements GetTableHandler {

    /**
     * Handles requests for public tables, where nothing is filtered.
     */
    @Override
    public void getTable(
        DatabaseConnection conn,
        RequestSource source,
        StreamableOutput out,
        boolean provideProgress,
        Table.TableID tableID,
        User masterUser,
        UserHost[] masterServers
    ) throws IOException, SQLException {
      getTablePublic(conn, source, out, provideProgress, tableID);
    }

    /**
     * Handles the request for a public table.
     */
    protected abstract void getTablePublic(
        DatabaseConnection conn,
        RequestSource source,
        StreamableOutput out,
        boolean provideProgress,
        Table.TableID tableID
    ) throws IOException, SQLException;
  }

  public abstract static class GetTableHandlerPermission implements GetTableHandler {

    /**
     * Gets the permission that is required to query this table.
     */
    protected abstract Permission.Name getPermissionName();

    /**
     * Checks if has permission, writing an empty table when does not have the permission.
     *
     * @see  AccountHandler#hasPermission(com.aoapps.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.aoserv.client.master.Permission.Name)
     */
    @Override
    public final void getTable(
        DatabaseConnection conn,
        RequestSource source,
        StreamableOutput out,
        boolean provideProgress,
        Table.TableID tableID,
        User masterUser,
        UserHost[] masterServers
    ) throws IOException, SQLException {
      if (AccountHandler.hasPermission(conn, source, getPermissionName())) {
        getTableHasPermission(conn, source, out, provideProgress, tableID, masterUser, masterServers);
      } else {
        // No permission, write empty table
        MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
      }
    }

    /**
     * Handles a request for the given table, once permission has been verified.
     *
     * @see  AccountHandler#hasPermission(com.aoapps.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.aoserv.client.master.Permission.Name)
     */
    protected abstract void getTableHasPermission(
        DatabaseConnection conn,
        RequestSource source,
        StreamableOutput out,
        boolean provideProgress,
        Table.TableID tableID,
        User masterUser,
        UserHost[] masterServers
    ) throws IOException, SQLException;
  }

  public abstract static class GetTableHandlerPermissionByRole extends GetTableHandlerPermission {

    /**
     * Calls role-specific implementations.
     */
    @Override
    protected void getTableHasPermission(
        DatabaseConnection conn,
        RequestSource source,
        StreamableOutput out,
        boolean provideProgress,
        Table.TableID tableID,
        User masterUser,
        UserHost[] masterServers
    ) throws IOException, SQLException {
      if (masterUser != null) {
        assert masterServers != null;
        if (masterServers.length == 0) {
          getTableMasterHasPermission(conn, source, out, provideProgress, tableID, masterUser);
        } else {
          getTableDaemonHasPermission(conn, source, out, provideProgress, tableID, masterUser, masterServers);
        }
      } else {
        getTableAdministratorHasPermission(conn, source, out, provideProgress, tableID);
      }
    }

    /**
     * Handles a {@link User master user} request for the given table, once
     * permission has been verified, with access to all {@link Account accounts}
     * and {@link Host hosts}.
     *
     * @see  AccountHandler#hasPermission(com.aoapps.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.aoserv.client.master.Permission.Name)
     */
    protected abstract void getTableMasterHasPermission(
        DatabaseConnection conn,
        RequestSource source,
        StreamableOutput out,
        boolean provideProgress,
        Table.TableID tableID,
        User masterUser
    ) throws IOException, SQLException;

    /**
     * Handles a {@link User master user} request for the given table, once
     * permission has been verified, with access limited to a set of
     * {@link Host hosts}.  This is the filtering generally used by
     * <a href="https://aoindustries.com/aoserv/daemon/">AOServ Daemon</a>.
     *
     * @see  AccountHandler#hasPermission(com.aoapps.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.aoserv.client.master.Permission.Name)
     */
    protected abstract void getTableDaemonHasPermission(
        DatabaseConnection conn,
        RequestSource source,
        StreamableOutput out,
        boolean provideProgress,
        Table.TableID tableID,
        User masterUser,
        UserHost[] masterServers
    ) throws IOException, SQLException;

    /**
     * Handles an {@link Administrator} request for the given table, once
     * permission has been verified, with access limited by their set of
     * {@link Account accounts} and the {@link Host hosts} those accounts
     * can access (see {@link AccountHost}.
     *
     * @see  AccountHandler#hasPermission(com.aoapps.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.aoserv.client.master.Permission.Name)
     */
    protected abstract void getTableAdministratorHasPermission(
        DatabaseConnection conn,
        RequestSource source,
        StreamableOutput out,
        boolean provideProgress,
        Table.TableID tableID
    ) throws IOException, SQLException;
  }

  /**
   * Gets an entire table.
   */
  public static void getTable(
      DatabaseConnection conn,
      RequestSource source,
      StreamableOutput out,
      boolean provideProgress,
      final Table.TableID tableID
  ) throws IOException, SQLException {
    com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
    User masterUser = MasterServer.getUser(conn, currentAdministrator);
    UserHost[] masterServers = masterUser == null ? null : MasterServer.getUserHosts(conn, currentAdministrator);

    GetTableHandler handler = getTableHandlers.get(tableID);
    if (handler != null) {
      // TODO: release conn before writing to out
      handler.getTable(conn, source, out, provideProgress, tableID, masterUser, masterServers);
    } else {
      throw new IOException("No " + GetTableHandler.class.getSimpleName() + " registered for table ID: " + tableID);
    }
  }

  /**
   * Gets an old table given its table name.
   * This is used for backwards compatibility to provide data for tables that no
   * longer exist.
   */
  public static void getOldTable(
      DatabaseConnection conn,
      RequestSource source,
      StreamableOutput out,
      boolean provideProgress,
      String tableName
  ) throws IOException, SQLException {
    switch (tableName) {
      case "billing.whois_history" :
        // Dispatch to new name WhoisHistory, which provides compatibility
        getTable(conn, source, out, provideProgress, Table.TableID.WhoisHistory);
        break;
      case "mysql.mysql_reserved_words" :
        if (
            source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_100) >= 0
                && source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80) <= 0
        ) {
          @SuppressWarnings("deprecation")
          com.aoindustries.aoserv.client.mysql.Server.ReservedWord[] reservedWords = com.aoindustries.aoserv.client.mysql.Server.ReservedWord.values();
          conn.close(); // Don't hold database connection while writing response
          MasterServer.writeObjects(
              source,
              out,
              provideProgress,
              new AbstractList<>() {
                @Override
                public AOServWritable get(int index) {
                  return (out, clientVersion) ->
                      out.writeUTF(reservedWords[index].name().toLowerCase(Locale.ROOT));
                }
                @Override
                public int size() {
                  return reservedWords.length;
                }
              }
          );
          return;
        }
        // fall-through to empty response
        break;
      case "net.net_protocols" :
        if (
            source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_100) >= 0
                && source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80) <= 0
        ) {
          // Send in lowercase
          com.aoapps.net.Protocol[] netProtocols = com.aoapps.net.Protocol.values();
          conn.close(); // Don't hold database connection while writing response
          MasterServer.writeObjects(
              source,
              out,
              provideProgress,
              new AbstractList<>() {
                @Override
                public AOServWritable get(int index) {
                  return (out, clientVersion) ->
                      out.writeUTF(netProtocols[index].name().toLowerCase(Locale.ROOT));
                }
                @Override
                public int size() {
                  return netProtocols.length;
                }
              }
          );
          return;
        }
        // fall-through to empty response
        break;
      case "postgresql.postgres_reserved_words" :
        if (
            source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_100) >= 0
                && source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80) <= 0
        ) {
          @SuppressWarnings("deprecation")
          com.aoindustries.aoserv.client.postgresql.Server.ReservedWord[] reservedWords = com.aoindustries.aoserv.client.postgresql.Server.ReservedWord.values();
          conn.close(); // Don't hold database connection while writing response
          MasterServer.writeObjects(
              source,
              out,
              provideProgress,
              new AbstractList<>() {
                @Override
                public AOServWritable get(int index) {
                  return (out, clientVersion) ->
                      out.writeUTF(reservedWords[index].name().toLowerCase(Locale.ROOT));
                }
                @Override
                public int size() {
                  return reservedWords.length;
                }
              }
          );
          return;
        }
        // fall-through to empty response
        break;
      case "web.httpd_site_bind_redirects" :
        // Dispatch to new name RewriteRule, which provides compatibility
        getTable(conn, source, out, provideProgress, Table.TableID.RewriteRule);
        break;
    }
    // Not recognized table name and version range: write empty response
    conn.close(); // Don't hold database connection while writing response
    MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
  }

  public static void invalidate(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      Table.TableID tableID,
      int host
  ) throws SQLException, IOException {
    checkInvalidator(conn, source, "invalidate");
    invalidateList.addTable(conn,
        tableID,
        InvalidateList.allAccounts,
        host == -1 ? InvalidateList.allHosts : InvalidateList.getHostCollection(host),
        true
    );
  }

  public static void checkInvalidator(DatabaseConnection conn, RequestSource source, String action) throws IOException, SQLException {
    if (!isInvalidator(conn, source)) {
      throw new SQLException("Table invalidation not allowed, '" + action + "'");
    }
  }

  public static boolean isInvalidator(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
    User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
    return mu != null && mu.canInvalidateTables();
  }

  private static final Object tableNamesLock = new Object();
  private static Map<Integer, String> tableNames;

  /**
   * Gets the table name, with schema prefixed.
   *
   * @see  #getTableName(com.aoapps.dbc.DatabaseAccess, com.aoindustries.aoserv.client.Table.TableID)
   */
  public static String getTableNameForDBTableID(DatabaseAccess db, Integer dbTableId) throws SQLException {
    synchronized (tableNamesLock) {
      if (tableNames == null) {
        tableNames = db.queryCall(
            results -> {
              Map<Integer, String> newMap = new HashMap<>();
              while (results.next()) {
                Integer id = results.getInt("id");
                String schema = results.getString("schema");
                String name = results.getString("name");
                if (newMap.put(id, schema + "." + name) != null) {
                  throw new SQLException("Duplicate id: " + id);
                }
              }
              return newMap;
            },
            "select\n"
                + "  s.\"name\" as \"schema\",\n"
                + "  t.id,\n"
                + "  t.\"name\"\n"
                + "from\n"
                + "  \"schema\".\"Table\" t\n"
                + "  inner join \"schema\".\"Schema\" s on t.\"schema\" = s.id"
        );
      }
      return tableNames.get(dbTableId);
    }
  }

  /**
   * Gets the table name, with schema prefixed.
   *
   * @see  #getTableNameForDBTableID(com.aoapps.dbc.DatabaseAccess, java.lang.Integer)
   */
  public static String getTableName(DatabaseAccess db, Table.TableID tableID) throws IOException, SQLException {
    return getTableNameForDBTableID(
        db,
        convertClientTableIDToDBTableID(
            db,
            AoservProtocol.Version.CURRENT_VERSION,
            tableID.ordinal()
        )
    );
  }

  private static final EnumMap<AoservProtocol.Version, Map<Integer, Integer>> fromClientTableIDs = new EnumMap<>(AoservProtocol.Version.class);

  /**
   * Converts a specific AoservProtocol version table ID to the number used in the database storage.
   *
   * @return  the {@code id} used in the database or {@code -1} if unknown
   */
  public static int convertClientTableIDToDBTableID(
      DatabaseAccess db,
      AoservProtocol.Version version,
      int clientTableID
  ) throws IOException, SQLException {
    synchronized (fromClientTableIDs) {
      Map<Integer, Integer> tableIDs = fromClientTableIDs.get(version);
      if (tableIDs == null) {
        IntList clientTables = db.queryIntList(
            "select\n"
                + "  st.id\n"
                + "from\n"
                + "  \"schema\".\"AoservProtocol\" client_ap,\n"
                + "  \"schema\".\"Table\"          st\n"
                + "  inner join \"schema\".\"AoservProtocol\" \"sinceVersion\" on st.\"sinceVersion\" = \"sinceVersion\".version\n"
                + "  left  join \"schema\".\"AoservProtocol\"  \"lastVersion\" on st.\"lastVersion\"  =  \"lastVersion\".version\n"
                + "where\n"
                + "  client_ap.version=?\n"
                + "  and client_ap.created >= \"sinceVersion\".created\n"
                + "  and (\"lastVersion\".created is null or client_ap.created <= \"lastVersion\".created)\n"
                + "order by\n"
                + "  st.id",
            version.getVersion()
        );
        int numTables = clientTables.size();
        tableIDs = AoCollections.newHashMap(numTables);
        for (int c = 0; c < numTables; c++) {
          tableIDs.put(c, clientTables.getInt(c));
        }
        fromClientTableIDs.put(version, tableIDs);
      }
      Integer i = tableIDs.get(clientTableID);
      return (i == null) ? -1 : i;
    }
  }

  private static final EnumMap<AoservProtocol.Version, Map<Integer, Integer>> toClientTableIDs = new EnumMap<>(AoservProtocol.Version.class);

  public static int convertDBTableIDToClientTableID(
      DatabaseAccess db,
      AoservProtocol.Version version,
      int tableID
  ) throws IOException, SQLException {
    synchronized (toClientTableIDs) {
      Map<Integer, Integer> clientTableIDs = toClientTableIDs.get(version);
      if (clientTableIDs == null) {
        IntList clientTables = db.queryIntList(
            "select\n"
                + "  st.id\n"
                + "from\n"
                + "  \"schema\".\"AoservProtocol\" client_ap,\n"
                + "             \"schema\".\"Table\"                      st\n"
                + "  inner join \"schema\".\"AoservProtocol\" \"sinceVersion\" on st.\"sinceVersion\" = \"sinceVersion\".version\n"
                + "  left  join \"schema\".\"AoservProtocol\"  \"lastVersion\" on st.\"lastVersion\"  = \"lastVersion\".version\n"
                + "where\n"
                + "  client_ap.version=?\n"
                + "  and client_ap.created >= \"sinceVersion\".created\n"
                + "  and (\"lastVersion\".created is null or client_ap.created <= \"lastVersion\".created)\n"
                + "order by\n"
                + "  st.id",
            version.getVersion()
        );
        int numTables = clientTables.size();
        clientTableIDs = AoCollections.newHashMap(numTables);
        for (int c = 0; c < numTables; c++) {
          clientTableIDs.put(clientTables.getInt(c), c);
        }
        toClientTableIDs.put(version, clientTableIDs);
      }
      Integer i = clientTableIDs.get(tableID);
      int clientTableID = (i == null) ? -1 : i;
      return clientTableID;
    }
  }

  /**
   * Converts the client's AoservProtocol-version-specific table ID to the version used by the master's AoservProtocol version.
   *
   * @return  The <code>Table.TableID</code> or <code>null</code> if no match.
   */
  public static Table.TableID convertFromClientTableID(
      DatabaseConnection conn,
      RequestSource source,
      int clientTableID
  ) throws IOException, SQLException {
    int dbTableID = convertClientTableIDToDBTableID(conn, source.getProtocolVersion(), clientTableID);
    if (dbTableID == -1) {
      return null;
    }
    int tableID = convertDBTableIDToClientTableID(conn, AoservProtocol.Version.CURRENT_VERSION, dbTableID);
    if (tableID == -1) {
      return null;
    }
    return _tableIDs[tableID];
  }

  /**
   * Converts a local (Master AoservProtocol) table ID to a client-version matched table ID.
   */
  public static int convertToClientTableID(
      DatabaseAccess db,
      RequestSource source,
      Table.TableID tableID
  ) throws IOException, SQLException {
    int dbTableID = convertClientTableIDToDBTableID(db, AoservProtocol.Version.CURRENT_VERSION, tableID.ordinal());
    if (dbTableID == -1) {
      return -1;
    }
    return convertDBTableIDToClientTableID(db, source.getProtocolVersion(), dbTableID);
  }

  private static final EnumMap<AoservProtocol.Version, Map<Table.TableID, Map<String, Integer>>> clientColumnIndexes = new EnumMap<>(AoservProtocol.Version.class);

  /*
   * 2018-11-18: This method appears unused.
   * If need to bring it back, see the "TODO" note below about a likely bug.
   * Also note that index is now a smallint/short.
  public static int getClientColumnIndex(
    DatabaseConnection conn,
    RequestSource source,
    Table.TableID tableID,
    String columnName
  ) throws IOException, SQLException {
    // Get the list of resolved tables for the requested version
    AoservProtocol.Version version = source.getProtocolVersion();
    synchronized (clientColumnIndexes) {
      Map<Table.TableID, Map<String, Integer>> tables = clientColumnIndexes.get(version);
      if (tables == null) {
        clientColumnIndexes.put(version, tables = new EnumMap<>(Table.TableID.class));
      }

      // Find the list of columns for this table
      Map<String, Integer> columns = tables.get(tableID);
      if (columns == null) {
        // TODO: Why is tableID not used in this query???
        List<String> clientColumns = conn.queryStringList(
          "select\n"
          + "  sc.\"name\"\n"
          + "from\n"
          + "  \"schema\".\"AoservProtocol\" client_ap,\n"
          + "             \"schema\".\"Column\"                     sc\n"
          + "  inner join \"schema\".\"AoservProtocol\" \"sinceVersion\" on sc.\"sinceVersion\" = \"sinceVersion\".version\n"
          + "  left  join \"schema\".\"AoservProtocol\"  \"lastVersion\" on sc.\"lastVersion\"  =  \"lastVersion\".version\n"
          + "where\n"
          + "  client_ap.version=?\n"
          + "  and client_ap.created >= \"sinceVersion\".created\n"
          + "  and (\"lastVersion\".created is null or client_ap.created <= \"lastVersion\".created)\n"
          + "order by\n"
          + "  sc.index",
          version.getVersion()
        );
        int numColumns = clientColumns.size();
        columns = AoCollections.HashMap(numColumns);
        for (int c = 0; c < numColumns; c++) {
          columns.put(clientColumns.get(c), c);
        }
        tables.put(tableID, columns);
      }

      // Return the column or -1 if not found
      Integer columnIndex = columns.get(columnName);
      return (columnIndex == null) ? -1 : columnIndex;
    }
  }
   */

  public static void invalidateTable(Table.TableID tableID) {
    if (tableID == Table.TableID.SCHEMA_TABLES) {
      synchronized (tableNamesLock) {
        tableNames = null;
      }
    }
    if (tableID == Table.TableID.AOSERV_PROTOCOLS || tableID == Table.TableID.SCHEMA_TABLES) {
      synchronized (fromClientTableIDs) {
        fromClientTableIDs.clear();
      }
      synchronized (toClientTableIDs) {
        toClientTableIDs.clear();
      }
    }
    if (tableID == Table.TableID.AOSERV_PROTOCOLS || tableID == Table.TableID.SCHEMA_COLUMNS) {
      synchronized (clientColumnIndexes) {
        clientColumnIndexes.clear();
      }
    }
  }

  // TODO: Move to proper service class
  public static IntList getOperatingSystemVersions(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
    return conn.queryIntList(
        "select distinct\n"
            + "  se.operating_system_version\n"
            + "from\n"
            + "  master.\"UserHost\" ms,\n"
            + "  linux.\"Server\" ao,\n"
            + "  net.\"Host\" se,\n"
            + "  distribution.\"OperatingSystemVersion\" osv\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=ao.server\n"
            + "  and ao.server=se.id\n"
            + "  and se.operating_system_version=osv.id\n"
            + "  and osv.is_aoserv_daemon_supported",
        source.getCurrentAdministrator()
    );
  }
}
