/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024  AO Industries, Inc.
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

package com.aoindustries.aoserv.master.billing;

import com.aoapps.collections.AoCollections;
import com.aoapps.cron.CronDaemon;
import com.aoapps.cron.CronJob;
import com.aoapps.cron.Schedule;
import com.aoapps.dbc.DatabaseAccess;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.dbc.ExtraRowException;
import com.aoapps.dbc.NoRowException;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoapps.hodgepodge.logging.ProcessTimer;
import com.aoapps.hodgepodge.util.Tuple2;
import com.aoapps.lang.ProcessResult;
import com.aoapps.lang.validation.ValidationException;
import com.aoapps.net.DomainName;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.billing.WhoisHistory;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.AoservMaster;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.InvalidateList;
import com.aoindustries.aoserv.master.MasterDatabase;
import com.aoindustries.aoserv.master.MasterService;
import com.aoindustries.aoserv.master.ObjectFactories;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles all the accesses to the whois history.
 *
 * @author  AO Industries, Inc.
 */
// TODO: Also do whois history for:
//       SmtpSmartHost?
//       Server.hostname?
//       CyrusImapdBind.servername
//       CyrusImapdServer.servername
//       SendmailServer
//       ftp.PrivateServer.hostname
//       IpAddress.hostname (for unmanaged servers, too?
public final class WhoisHistoryService implements MasterService {

  private static final Logger logger = Logger.getLogger(WhoisHistoryService.class.getName());

  private static final boolean DEBUG = false;

  @Override
  public void start() {
    CronDaemon.addCronJob(cronJob, logger);
    // Run at start-up, too
    CronDaemon.runImmediately(cronJob);
  }

  // <editor-fold desc="Clean-up" defaultstate="collapsed">
  /**
   * The amount of time to keep whois history, used as a PostgreSQL interval.
   */
  private static final String
      CLEANUP_AFTER_GOOD_ACCOUNT = "7 years", // Was 1 year, is this overkill?
      CLEANUP_AFTER_CLOSED_ACCOUNT_ZERO_BALANCE = "7 years", // Was 1 year, is this overkill?
      CLEANUP_AFTER_CLOSED_ACCOUNT_NO_TRANSACTIONS = "1 year";

  private static void cleanup(DatabaseConnection conn, InvalidateList invalidateList) throws IOException, SQLException {
    Set<Account.Name> accountsAffected = new HashSet<>();

    // Open account that have balance <= $0.00 and entry is older than one year
    List<Account.Name> deletedGoodStanding = conn.updateList(
        ObjectFactories.accountNameFactory,
        "DELETE FROM billing.\"WhoisHistoryAccount\" WHERE id IN (\n"
            + "  SELECT\n"
            + "    wha.id\n"
            + "  FROM\n"
            + "               billing.\"WhoisHistoryAccount\" wha\n"
            + "    INNER JOIN billing.\"WhoisHistory\"        wh  ON wha.\"whoisHistory\" = wh.id\n"
            + "    INNER JOIN account.\"Account\"             bu  ON wha.account          = bu.accounting\n"
            + "  WHERE\n"
            // entry is older than interval
            + "    (now() - wh.\"time\") > ?::interval\n"
            // open account
            + "    AND bu.canceled IS NULL\n"
            // balance is <= $0.00
            + "    AND (\n"
            + "      SELECT ab.balance FROM billing.account_balances ab\n"
            + "      WHERE bu.accounting = ab.accounting AND ab.balance > '0'::numeric\n"
            + "      LIMIT 1\n"
            + "    ) IS NULL\n"
            + ") RETURNING account",
        CLEANUP_AFTER_GOOD_ACCOUNT
    );
    if (!deletedGoodStanding.isEmpty()) {
      accountsAffected.addAll(deletedGoodStanding);
      if (DEBUG) {
        System.out.println(WhoisHistoryService.class.getSimpleName() + ": cleanup: Deleted good standing: " + deletedGoodStanding.size());
      }
    }

    // Closed account that have a balance of $0.00, has not had any billing.Transaction for interval, and entry is older than interval
    List<Account.Name> deletedCanceledZero = conn.updateList(
        ObjectFactories.accountNameFactory,
        "DELETE FROM billing.\"WhoisHistoryAccount\" WHERE id IN (\n"
            + "  SELECT\n"
            + "    wha.id\n"
            + "  FROM\n"
            + "               billing.\"WhoisHistoryAccount\" wha\n"
            + "    INNER JOIN billing.\"WhoisHistory\"        wh  ON wha.\"whoisHistory\" = wh.id\n"
            + "    INNER JOIN account.\"Account\"             bu  ON wha.account          = bu.accounting\n"
            + "  WHERE\n"
            // entry is older than interval
            + "    (now() - wh.\"time\") > ?::interval\n"
            // closed account
            + "    AND bu.canceled IS NOT NULL\n"
            // has not had any accounting billing.Transaction for interval
            + "    AND (SELECT tr.transid FROM billing.\"Transaction\" tr WHERE bu.accounting = tr.accounting AND tr.\"time\" >= (now() - ?::interval) LIMIT 1) IS NULL\n"
            // balance is $0.00
            + "    AND (\n"
            + "      SELECT ab.balance FROM billing.account_balances ab\n"
            + "      WHERE bu.accounting = ab.accounting AND ab.balance != '0'::numeric\n"
            + "      LIMIT 1\n"
            + "    ) IS NULL\n"
            + ") RETURNING account",
        CLEANUP_AFTER_CLOSED_ACCOUNT_ZERO_BALANCE,
        CLEANUP_AFTER_CLOSED_ACCOUNT_NO_TRANSACTIONS
    );
    if (!deletedCanceledZero.isEmpty()) {
      accountsAffected.addAll(deletedCanceledZero);
      if (DEBUG) {
        System.out.println(WhoisHistoryService.class.getSimpleName() + ": cleanup: Deleted canceled at zero balance: " + deletedCanceledZero.size());
      }
    }
    if (!accountsAffected.isEmpty()) {
      invalidateList.addTable(conn,
          Table.TableId.WhoisHistoryAccount,
          accountsAffected,
          InvalidateList.allHosts,
          false
      );
      // Affects visibility, so invalidate WhoisHistory, too
      invalidateList.addTable(conn,
          Table.TableId.WhoisHistory,
          accountsAffected,
          InvalidateList.allHosts,
          false
      );
    }
    // Cleanup any orphaned data
    conn.update("DELETE FROM billing.\"WhoisHistory\" WHERE id NOT IN (SELECT \"whoisHistory\" FROM billing.\"WhoisHistoryAccount\")");
  }
  // </editor-fold>

  // <editor-fold desc="CronJob" defaultstate="collapsed">

  private static final String COMMAND = "/usr/bin/whois";

  /**
   * The minimum time to sleep between lookups in millis.
   */
  private static final int LOOKUP_SLEEP_MINIMUM = 10 * 1000; // 10 seconds

  /**
   * The interval between checks.
   */
  private static final long RECHECK_MILLIS = 7L * 24 * 60 * 60 * 1000; // 7 days

  /**
   * The target time for processing pass completion.
   */
  private static final long PASS_COMPLETION_TARGET = RECHECK_MILLIS / 2; // Half the recheck time

  /**
   * The maximum time for a processing pass.
   */
  private static final long TIMER_MAX_TIME = RECHECK_MILLIS;

  /**
   * The interval in which the administrators will be reminded.
   */
  private static final long TIMER_REMINDER_INTERVAL = 24L * 60 * 60 * 1000; // 1 day

  /**
   * Runs hourly, 12 minutes after the hour.
   */
  private static final Schedule schedule = (minute, hour, dayOfMonth, month, dayOfWeek, year) ->
      minute == 12;

  private final CronJob cronJob = new CronJob() {

    @Override
    public Schedule getSchedule() {
      return schedule;
    }

    @Override
    public int getThreadPriority() {
      return Thread.NORM_PRIORITY - 1;
    }

    // TODO: Should we fire this off manually, or at least have a way to do so when the process fails?
    //
    // TODO: Should there be a monthly task to make sure this process is working correctly?
    //
    // TODO: This should probably go in a dns.monitoring schema, and be watched by NOC monitoring, with some older
    //       records left around for billing purposes like done here.
    @Override
    @SuppressWarnings("try")
    public void run(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
      try {
        try (
            ProcessTimer timer = new ProcessTimer(
                logger,
                getClass().getName(),
                "runCronJob",
                WhoisHistoryService.class.getSimpleName() + " - Whois History",
                "Looking up whois and cleaning old records",
                TIMER_MAX_TIME,
                TIMER_REMINDER_INTERVAL
            )
            ) {
          AoservMaster.executorService.submit(timer);

          // Start the transaction
          try (DatabaseConnection conn = MasterDatabase.getDatabase().connect()) {
            InvalidateList invalidateList = new InvalidateList();
            /*
             * Remove old records first
             */
            cleanup(conn, invalidateList);
            conn.commit();
            AoservMaster.invalidateTables(conn, invalidateList, null);
            invalidateList.reset();

            /*
             * The add new records
             */
            // Get the set of unique registrable domains and accounts in the system
            Map<DomainName, Set<Account.Name>> registrableDomains = getWhoisHistoryDomains(conn);
            conn.close(); // Don't hold database connection while sleeping

            // Find the number of distinct registrable domains
            int registrableDomainCount = registrableDomains.size();
            if (registrableDomainCount > 0) {
              // Compute target sleep time as if we have to do all, despite we will probably not do all.
              // This keeps the scheduling such that the cron job will not slow down too much, thus having to catch-up later
              final long targetSleepTime = PASS_COMPLETION_TARGET / registrableDomainCount;
              if (DEBUG) {
                System.out.println(
                    WhoisHistoryService.class.getSimpleName()
                        + ": Target sleep time for "
                        + registrableDomainCount
                        + " registrable "
                        + (registrableDomainCount == 1 ? "domain" : "domains")
                        + " is " + targetSleepTime + " ms"
                );
              }

              // Sort the domains by those never looked up first, then by their order from oldest to newest/
              // The list is not pruned yet, because it might become time while slowly processing the list
              // Timestamp null when never looked-up
              Map<DomainName, Timestamp> lookupOrder;
                {
                  // Lookup the most recent time for all previously logged registrable domains, ordered by oldest first
                  final Map<DomainName, Timestamp> lastChecked = conn.queryCall(
                      results -> {
                        try {
                          // Minimize early rehashes, perfect fit if only registrableDomainCount will be returned
                          Map<DomainName, Timestamp> map = AoCollections.newLinkedHashMap(registrableDomainCount);
                          int oldNotUsedCount = 0;
                          while (results.next()) {
                            DomainName registrableDomain = DomainName.valueOf(results.getString(1));
                            if (registrableDomains.keySet().contains(registrableDomain)) {
                              map.put(
                                  registrableDomain,
                                  results.getTimestamp(2)
                              );
                            } else {
                              oldNotUsedCount++;
                            }
                          }
                          if (DEBUG) {
                            System.out.println(WhoisHistoryService.class.getSimpleName() + ": Old not used now count: " + oldNotUsedCount
                                + ", if this becomes a large value, might be worth doing a WHERE \"registrableDomain\" IN (...)");
                          }
                          return map;
                        } catch (ValidationException e) {
                          throw new SQLException(e);
                        }
                      },
                      // TODO: We could send a WHERE "registrableDomain" IN (...), but this is less code now
                      "select \"registrableDomain\", max(\"time\") from billing.\"WhoisHistory\" group by \"registrableDomain\" order by max"
                  );
                  lookupOrder = AoCollections.newLinkedHashMap(registrableDomainCount);
                  for (DomainName registrableDomain : registrableDomains.keySet()) {
                    if (!lastChecked.containsKey(registrableDomain)) {
                      lookupOrder.put(registrableDomain, null);
                    }
                  }
                  if (DEBUG) {
                    System.out.println(WhoisHistoryService.class.getSimpleName() + ": Number never checked: " + lookupOrder.size());
                  }
                  lookupOrder.putAll(lastChecked);
                  assert registrableDomains.keySet().equals(lookupOrder.keySet());
                }
              // Performs the whois lookup once per unique registrable domain
              try {
                for (Map.Entry<DomainName, Timestamp> entry : lookupOrder.entrySet()) {
                  final DomainName registrableDomain = entry.getKey();
                  final Timestamp time = entry.getValue();
                  final boolean checkNow;
                  if (time == null) {
                    checkNow = true;
                    if (DEBUG) {
                      System.out.println(WhoisHistoryService.class.getSimpleName() + ": " + registrableDomain + ": Never checked, checkNow: " + checkNow);
                    }
                  } else {
                    long timeSince = System.currentTimeMillis() - time.getTime();
                    checkNow = timeSince >= RECHECK_MILLIS || timeSince <= -RECHECK_MILLIS;
                    if (DEBUG) {
                      System.out.println(WhoisHistoryService.class.getSimpleName() + ": " + registrableDomain + ": Last checked " + time + ", checkNow: " + checkNow);
                    }
                  }
                  // Since they are in order by time, quite once the first one to not check is found
                  if (!checkNow) {
                    if (DEBUG) {
                      System.out.println(WhoisHistoryService.class.getSimpleName() + ": checkNow is false, it is not time for any remaining in the list, breaking loop now");
                    }
                    break;
                  }
                  final long startTime = System.currentTimeMillis();
                  Integer exitStatus;
                  String output;
                  String error;
                  try {
                    String lower = registrableDomain.toLowerCase();
                    ProcessResult result = ProcessResult.exec(COMMAND, "-H", lower);
                    exitStatus = result.getExitVal();
                    output = result.getStdout();
                    error = result.getStderr();
                    if (DEBUG) {
                      System.out.println(WhoisHistoryService.class.getSimpleName() + ": " + registrableDomain + ": Success");
                    }
                  } catch (ThreadDeath td) {
                    throw td;
                  } catch (Throwable t) {
                    logger.log(Level.FINE, null, t);
                    exitStatus = null;
                    output = "";
                    error = t.toString();
                    if (DEBUG) {
                      System.out.println(WhoisHistoryService.class.getSimpleName() + ": " + registrableDomain + ": Error");
                    }
                  }
                  // update database
                  // TODO: Store the parsed nameservers, too?  At least for when is success.
                  // This could be a batch, but this is short and simple
                  int whoisHistory = conn.updateInt(
                      "INSERT INTO billing.\"WhoisHistory\" (\"registrableDomain\", \"exitStatus\", \"output\", error) VALUES (?,?,?,?) RETURNING id",
                      registrableDomain,
                      exitStatus == null ? DatabaseAccess.Null.INTEGER : exitStatus,
                      output,
                      error
                  );
                  Set<Account.Name> accounts = registrableDomains.get(registrableDomain);
                  for (Account.Name account : accounts) {
                    conn.update(
                        "insert into billing.\"WhoisHistoryAccount\" (\"whoisHistory\", account) values(?,?)",
                        whoisHistory,
                        account
                    );
                  }
                  invalidateList.addTable(conn,
                      Table.TableId.WhoisHistory,
                      accounts,
                      InvalidateList.allHosts,
                      false
                  );
                  invalidateList.addTable(conn,
                      Table.TableId.WhoisHistoryAccount,
                      accounts,
                      InvalidateList.allHosts,
                      false
                  );
                  conn.commit();
                  AoservMaster.invalidateTables(conn, invalidateList, null);
                  conn.close(); // Don't hold database connection while sleeping
                  invalidateList.reset();
                  long sleepTime = targetSleepTime - (System.currentTimeMillis() - startTime);
                  if (sleepTime < LOOKUP_SLEEP_MINIMUM) {
                    sleepTime = LOOKUP_SLEEP_MINIMUM;
                  }
                  if (DEBUG) {
                    System.out.println(WhoisHistoryService.class.getSimpleName() + ": " + registrableDomain + ": Completed, sleeping " + sleepTime + " ms");
                  }
                  Thread.sleep(sleepTime);
                }
              } catch (InterruptedException e) {
                logger.log(Level.WARNING, null, e);
                // Restore the interrupted status
                Thread.currentThread().interrupt();
              }
            } else if (DEBUG) {
              System.out.println(WhoisHistoryService.class.getSimpleName() + ": No registrable domains");
            }
            conn.commit();
            AoservMaster.invalidateTables(conn, invalidateList, null);
          }
        }
      } catch (ThreadDeath td) {
        throw td;
      } catch (Throwable t) {
        logger.log(Level.SEVERE, null, t);
      }
    }
  };

  /**
   * Gets the set of all unique registrable domains (single domain label + public suffix) and accounts.
   * Merges the results of calling {@link WhoisHistoryDomainLocator#getWhoisHistoryDomains(com.aoapps.dbc.DatabaseConnection)}
   * on all {@link MasterService services}.
   */
  private Map<DomainName, Set<Account.Name>> getWhoisHistoryDomains(DatabaseConnection conn) throws IOException, SQLException {
    Map<DomainName, Set<Account.Name>> merged = new HashMap<>();
    for (WhoisHistoryDomainLocator locator : AoservMaster.getServices(WhoisHistoryDomainLocator.class)) {
      for (Map.Entry<DomainName, Set<Account.Name>> entry : locator.getWhoisHistoryDomains(conn).entrySet()) {
        DomainName registrableDomain = entry.getKey();
        Set<Account.Name> accounts = merged.get(registrableDomain);
        if (accounts == null) {
          merged.put(registrableDomain, accounts = new LinkedHashSet<>());
        }
        accounts.addAll(entry.getValue());

      }
    }
    return merged;
  }

  // </editor-fold>

  /**
   * Gets the whois output and error for the specific billing.WhoisHistory record.
   *
   * <p>The same filtering as {@link #startGetTableHandler()}</p>
   */
  public Tuple2<String, String> getWhoisHistoryOutput(DatabaseConnection conn, RequestSource source, int whoisHistoryAccount) throws IOException, SQLException {
    com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
    User masterUser = AoservMaster.getUser(conn, currentAdministrator);
    if (masterUser != null) {
      UserHost[] masterServers = AoservMaster.getUserHosts(conn, currentAdministrator);
      if (masterServers.length == 0) {
        if (source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_18) <= 0) {
          // id is that of the associated billing.WhoisHistoryAccount
          return conn.queryCall(
              results -> {
                if (results.next()) {
                  Tuple2<String, String> t = new Tuple2<>(results.getString(1), results.getString(2));
                  if (results.next()) {
                    throw new ExtraRowException(results);
                  }
                  return t;
                } else {
                  throw new NoRowException();
                }
              },
              "SELECT\n"
                  + "  wh.\"output\",\n"
                  + "  wh.error\n"
                  + "FROM\n"
                  + "  billing.\"WhoisHistory\"                   wh\n"
                  + "  INNER JOIN billing.\"WhoisHistoryAccount\" wha ON wh.id = wha.\"whoisHistory\"\n"
                  + "WHERE\n"
                  + "  wha.id=?",
              whoisHistoryAccount
          );
        } else {
          return conn.queryCall(
              results -> {
                if (results.next()) {
                  Tuple2<String, String> t = new Tuple2<>(results.getString(1), results.getString(2));
                  if (results.next()) {
                    throw new ExtraRowException(results);
                  }
                  return t;
                } else {
                  throw new NoRowException();
                }
              },
              "select \"output\", error from billing.\"WhoisHistory\" where id=?",
              whoisHistoryAccount
          );
        }
      } else {
        throw new NoRowException();
      }
    } else {
      if (source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_18) <= 0) {
        // id is that of the associated billing.WhoisHistoryAccount
        return conn.queryCall(
            results -> {
              if (results.next()) {
                Tuple2<String, String> t = new Tuple2<>(results.getString(1), results.getString(2));
                if (results.next()) {
                  throw new ExtraRowException(results);
                }
                return t;
              } else {
                throw new NoRowException();
              }
            },
            "SELECT\n"
                + "  wh.\"output\",\n"
                + "  wh.error\n"
                + "FROM\n"
                + "  account.\"User\" un,\n"
                + "  billing.\"Package\" pk,\n"
                + TableHandler.BU1_PARENTS_JOIN
                + "  billing.\"WhoisHistoryAccount\" wha,\n"
                + "  billing.\"WhoisHistory\" wh\n"
                + "WHERE\n"
                + "  un.username=?\n"
                + "  AND un.package=pk.name\n"
                + "  AND (\n"
                + TableHandler.PK_BU1_PARENTS_WHERE
                + "  )\n"
                + "  AND bu1.accounting = wha.account\n"
                + "  AND wha.\"whoisHistory\" = wh.id\n"
                + "  AND wha.id=?",
            currentAdministrator,
            whoisHistoryAccount
        );
      } else {
        return conn.queryCall(
            results -> {
              if (results.next()) {
                Tuple2<String, String> t = new Tuple2<>(results.getString(1), results.getString(2));
                if (results.next()) {
                  throw new ExtraRowException(results);
                }
                return t;
              } else {
                throw new NoRowException();
              }
            },
            "SELECT\n"
                + "  wh.\"output\",\n"
                + "  wh.error\n"
                + "FROM\n"
                + "  account.\"User\" un,\n"
                + "  billing.\"Package\" pk,\n"
                + TableHandler.BU1_PARENTS_JOIN
                + "  billing.\"WhoisHistoryAccount\" wha,\n"
                + "  billing.\"WhoisHistory\" wh\n"
                + "WHERE\n"
                + "  un.username=?\n"
                + "  AND un.package=pk.name\n"
                + "  AND (\n"
                + TableHandler.PK_BU1_PARENTS_WHERE
                + "  )\n"
                + "  AND bu1.accounting = wha.account\n"
                + "  AND wha.\"whoisHistory\" = wh.id\n"
                + "  AND wh.id=?\n"
                + "LIMIT 1", // Might be reached through multiple accounts
            currentAdministrator,
            whoisHistoryAccount
        );
      }
    }
  }

  // <editor-fold desc="GetTableHandler" defaultstate="collapsed">
  @Override
  public TableHandler.GetTableHandler startGetTableHandler() {
    return new TableHandler.GetTableHandlerByRole() {

      @Override
      public Set<Table.TableId> getTableIds() {
        return EnumSet.of(Table.TableId.WhoisHistory);
      }

      @Override
      @SuppressWarnings("deprecation")
      protected void getTableMaster(
          DatabaseConnection conn,
          RequestSource source,
          StreamableOutput out,
          boolean provideProgress,
          Table.TableId tableId,
          User masterUser
      ) throws IOException, SQLException {
        if (source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_18) <= 0) {
          // Use join and id from WhoisHistoryAccount
          AoservMaster.writeObjects(
              conn,
              source,
              out,
              provideProgress,
              CursorMode.FETCH,
              new WhoisHistory(),
              "SELECT\n"
                  + "  wha.id,\n"
                  + "  wh.\"registrableDomain\",\n"
                  + "  wh.\"time\",\n"
                  + "  wh.\"exitStatus\",\n"
                  // Protocol conversion
                  + "  wha.account AS accounting\n"
                  + "FROM\n"
                  + "  billing.\"WhoisHistory\"                   wh\n"
                  + "  INNER JOIN billing.\"WhoisHistoryAccount\" wha ON wh.id = wha.\"whoisHistory\""
          );
        } else {
          AoservMaster.writeObjects(
              conn,
              source,
              out,
              provideProgress,
              CursorMode.FETCH,
              new WhoisHistory(),
              "select\n"
                  + "  id,\n"
                  + "  \"registrableDomain\",\n"
                  + "  \"time\",\n"
                  + "  \"exitStatus\",\n"
                  // Protocol conversion
                  + "  null as accounting\n"
                  + "from\n"
                  + "  billing.\"WhoisHistory\""
          );
        }
      }

      @Override
      protected void getTableDaemon(
          DatabaseConnection conn,
          RequestSource source,
          StreamableOutput out,
          boolean provideProgress,
          Table.TableId tableId,
          User masterUser,
          UserHost[] masterServers
      ) throws IOException, SQLException {
        // The servers don't need access to this information
        AoservMaster.writeObjects(source, out, provideProgress, Collections.emptyList());
      }

      @Override
      @SuppressWarnings("deprecation")
      protected void getTableAdministrator(
          DatabaseConnection conn,
          RequestSource source,
          StreamableOutput out,
          boolean provideProgress,
          Table.TableId tableId
      ) throws IOException, SQLException {
        if (source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_18) <= 0) {
          // Use join and id from WhoisHistoryAccount
          AoservMaster.writeObjects(
              conn,
              source,
              out,
              provideProgress,
              CursorMode.AUTO,
              new WhoisHistory(),
              "select\n"
                  + "  wha.id,\n"
                  + "  wh.\"registrableDomain\",\n"
                  + "  wh.\"time\",\n"
                  + "  wh.\"exitStatus\",\n"
                  // Protocol conversion
                  + "  wha.account as accounting\n"
                  + "from\n"
                  + "  account.\"User\" un,\n"
                  + "  billing.\"Package\" pk,\n"
                  + TableHandler.BU1_PARENTS_JOIN
                  + "  billing.\"WhoisHistoryAccount\" wha,\n"
                  + "  billing.\"WhoisHistory\" wh\n"
                  + "where\n"
                  + "  un.username=?\n"
                  + "  and un.package=pk.name\n"
                  + "  and (\n"
                  + TableHandler.PK_BU1_PARENTS_WHERE
                  + "  )\n"
                  + "  and bu1.accounting = wha.account\n"
                  + "  and wha.\"whoisHistory\" = wh.id",
              source.getCurrentAdministrator()
          );
        } else {
          AoservMaster.writeObjects(
              conn,
              source,
              out,
              provideProgress,
              CursorMode.AUTO,
              new WhoisHistory(),
              "select distinct\n"
                  + "  wh.id,\n"
                  + "  wh.\"registrableDomain\",\n"
                  + "  wh.\"time\",\n"
                  + "  wh.\"exitStatus\",\n"
                  // Protocol conversion
                  + "  null as accounting\n"
                  + "from\n"
                  + "  account.\"User\" un,\n"
                  + "  billing.\"Package\" pk,\n"
                  + TableHandler.BU1_PARENTS_JOIN
                  + "  billing.\"WhoisHistoryAccount\" wha,\n"
                  + "  billing.\"WhoisHistory\" wh\n"
                  + "where\n"
                  + "  un.username=?\n"
                  + "  and un.package=pk.name\n"
                  + "  and (\n"
                  + TableHandler.PK_BU1_PARENTS_WHERE
                  + "  )\n"
                  + "  and bu1.accounting = wha.account\n"
                  + "  and wha.\"whoisHistory\" = wh.id",
              source.getCurrentAdministrator()
          );
        }
      }
    };
  }
  // </editor-fold>
}
