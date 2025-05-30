/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2009-2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2025  AO Industries, Inc.
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
import com.aoapps.cron.CronDaemon;
import com.aoapps.cron.CronJob;
import com.aoapps.cron.Schedule;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.logging.ProcessTimer;
import com.aoapps.hodgepodge.util.Tuple3;
import com.aoapps.lang.Throwables;
import com.aoindustries.aoserv.client.linux.Server;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.daemon.client.AoservDaemonConnector;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The <code>ClusterHandler</code> maintains a mapping of virtual servers
 * to physical servers.  It updates its mapping every minute.
 *
 * @author  AO Industries, Inc.
 */
public final class ClusterHandler implements CronJob {

  private static final Logger logger = Logger.getLogger(ClusterHandler.class.getName());

  /**
   * The maximum time for a processing pass.
   */
  private static final long TIMER_MAX_TIME = 60L * 1000L; // Five minutes

  /**
   * The interval in which the administrators will be reminded.
   */
  private static final long TIMER_REMINDER_INTERVAL = 60L * 60L * 1000L; // One hour

  private static boolean started;

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static void start() {
    synchronized (System.out) {
      if (!started) {
        System.out.print("Starting " + ClusterHandler.class.getSimpleName() + ": ");
        CronDaemon.addCronJob(new ClusterHandler(), logger);
        started = true;
        System.out.println("Done");
        // Run immediately to populate mapping on start-up
        AoservMaster.executorService.submit(ClusterHandler::updateMappings);
      }
    }
  }

  private ClusterHandler() {
    // Do nothing
  }

  /**
   * Runs every minute.
   */
  private static final Schedule schedule = (int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) -> true;

  @Override
  public Schedule getSchedule() {
    return schedule;
  }

  @Override
  public int getThreadPriority() {
    return Thread.NORM_PRIORITY + 1;
  }

  @Override
  public void run(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
    updateMappings();
  }

  public static class ClusterException extends IOException {

    private static final long serialVersionUID = 1L;

    public ClusterException() {
      super();
    }

    public ClusterException(String message) {
      super(message);
    }

    public ClusterException(Throwable cause) {
      super(cause);
    }

    public ClusterException(String message, Throwable cause) {
      super(message, cause);
    }

    static {
      Throwables.registerSurrogateFactory(ClusterException.class, (template, cause) ->
          new ClusterException(template.getMessage(), cause)
      );
    }
  }

  private static final Object mappingsLock = new Object();

  /**
   * The set of virtual servers that have primary DRBD roles on a per physical
   * server basis.
   */
  private static Map<Integer, Set<Integer>> primaryMappings = Collections.emptyMap();

  /**
   * The set of virtual servers that have secondary DRBD roles on a per physical
   * server basis.
   */
  private static Map<Integer, Set<Integer>> secondaryMappings = Collections.emptyMap();

  /**
   * The set of virtual servers that have Xen auto start links on a per physical
   * server basis.
   */
  private static Map<Integer, Set<Integer>> autoMappings = Collections.emptyMap();

  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter") // private only
  private static void setMappings(
      Map<Integer, Set<Integer>> newPrimaryMappings,
      Map<Integer, Set<Integer>> newSecondaryMappings,
      Map<Integer, Set<Integer>> newAutoMappings
  ) {
    synchronized (mappingsLock) {
      primaryMappings = newPrimaryMappings;
      secondaryMappings = newSecondaryMappings;
      autoMappings = newAutoMappings;
    }
  }

  public static int getPrimaryPhysicalServer(DatabaseConnection conn, RequestSource source, int virtualServer) throws IOException, SQLException {
    // Must be a cluster admin
    checkClusterAdmin(conn, source, "getPrimaryPhysicalServer");
    return getPrimaryPhysicalServer(virtualServer);
  }

  /**
   * Gets the id of the physical server that is currently the primary for
   * the virtual server.  If there is no primary (Secondary/Secondary role),
   * will use the physical server that has Xen auto start configured.
   */
  public static int getPrimaryPhysicalServer(int virtualServer) throws ClusterException {
    Integer virtualServerInt = virtualServer;
    int physicalServer = -1;
    boolean physicalServerFound = false;
    synchronized (mappingsLock) {
      for (Map.Entry<Integer, Set<Integer>> entry : primaryMappings.entrySet()) {
        if (entry.getValue().contains(virtualServerInt)) {
          if (physicalServerFound) {
            throw new ClusterException("Virtual server #" + virtualServer + " primary found on more than one physical server");
          }
          physicalServer = entry.getKey();
          physicalServerFound = true;
        }
      }
      if (!physicalServerFound) {
        for (Map.Entry<Integer, Set<Integer>> entry : autoMappings.entrySet()) {
          if (entry.getValue().contains(virtualServerInt)) {
            if (physicalServerFound) {
              throw new ClusterException("Virtual server #" + virtualServer + " auto start link found on more than one physical server");
            }
            physicalServer = entry.getKey();
            physicalServerFound = true;
          }
        }
      }
    }
    if (!physicalServerFound) {
      throw new ClusterException("Virtual server #" + virtualServer + " primary not found on any physical server");
    }
    return physicalServer;
  }

  public static int getSecondaryPhysicalServer(DatabaseConnection conn, RequestSource source, int virtualServer) throws IOException, SQLException {
    // Must be a cluster admin
    checkClusterAdmin(conn, source, "getSecondaryPhysicalServer");
    return getSecondaryPhysicalServer(virtualServer);
  }

  /**
   * Gets the id of the physical server that is currently the secondary for
   * the virtual server.  If there are two secondaries (Secondary/Secondary role),
   * will use the physical server that does not have Xen auto start configured.
   */
  public static int getSecondaryPhysicalServer(int virtualServer) throws ClusterException {
    Integer virtualServerInt = virtualServer;
    synchronized (mappingsLock) {
      // Find the set of all physical servers that have this as secondary
      Set<Integer> physicalServers = new HashSet<>();
      for (Map.Entry<Integer, Set<Integer>> entry : secondaryMappings.entrySet()) {
        if (entry.getValue().contains(virtualServerInt)) {
          physicalServers.add(entry.getKey());
        }
      }
      if (physicalServers.isEmpty()) {
        // None found
        throw new ClusterException("Virtual server #" + virtualServer + " secondary not found on any physical server");
      } else if (physicalServers.size() == 1) {
        // If there is only one secondary, use it if not auto
        Integer physicalServer1 = physicalServers.iterator().next();
        Set<Integer> autoMappings1 = autoMappings.get(physicalServer1);
        boolean auto = autoMappings1 != null && autoMappings1.contains(virtualServer);
        if (auto) {
          throw new ClusterException("Virtual server #" + virtualServer + " secondary only found on physical server with auto start link: " + physicalServer1);
        }
        return physicalServer1;
      } else if (physicalServers.size() == 2) {
        // If two, choose the one that is not auto-start
        Iterator<Integer> iter = physicalServers.iterator();
        Integer physicalServer1 = iter.next();
        Integer physicalServer2 = iter.next();
        // Get the auto mappings for each
        Set<Integer> autoMappings1 = autoMappings.get(physicalServer1);
        Set<Integer> autoMappings2 = autoMappings.get(physicalServer2);
        // Find if has on auto
        boolean auto1 = autoMappings1 != null && autoMappings1.contains(virtualServer);
        boolean auto2 = autoMappings2 != null && autoMappings2.contains(virtualServer);
        // Resolve based on auto mappings
        if (auto1) {
          if (auto2) {
            // auto1 && auto2
            throw new ClusterException("Virtual server #" + virtualServer + " auto start link found on both physical servers: " + physicalServer1 + " and " + physicalServer2);
          } else {
            // auto1 && !auto2
            return physicalServer2;
          }
        } else {
          if (auto2) {
            // !auto1 && auto2
            return physicalServer1;
          } else {
            // !auto1 && !auto2
            throw new ClusterException("Virtual server #" + virtualServer + " auto start link not found on either physical server: " + physicalServer1 + " or " + physicalServer2);
          }
        }
      } else {
        // Error if more than two
        throw new ClusterException("Virtual server #" + virtualServer + " secondary found on more than two physical servers: " + physicalServers);
      }
    }
  }

  private static final Object updateMappingsLock = new Object();

  @SuppressWarnings({"UseSpecificCatch", "TooBroadCatch", "SleepWhileHoldingLock", "SleepWhileInLoop"})
  private static void updateMappings() {
    synchronized (updateMappingsLock) {
      try {
        try (
            ProcessTimer timer = new ProcessTimer(
                logger,
                ClusterHandler.class.getName(),
                "runCronJob",
                "ClusterHandler - Find Virtual Host Mapping",
                "Finding the current mapping of virtual servers onto physical servers",
                TIMER_MAX_TIME,
                TIMER_REMINDER_INTERVAL
            )
            ) {
          AoservMaster.executorService.submit(timer);

          // Query the servers in parallel
          final MasterDatabase database = MasterDatabase.getDatabase();
          IntList xenPhysicalServers = NetHostHandler.getEnabledXenPhysicalServers(database);
          Map<Integer, Future<Tuple3<Set<Integer>, Set<Integer>, Set<Integer>>>> futures = AoCollections.newHashMap(xenPhysicalServers.size());
          for (final Integer xenPhysicalServer : xenPhysicalServers) {
            futures.put(
                xenPhysicalServer,
                AoservMaster.executorService.submit(() -> {
                  // Try up to ten times
                  final int attempts = 10;
                  for (int c = 0; c < attempts; c++) {
                    try {
                      final int rootPackagePkey = PackageHandler.getIdForPackage(database, AccountHandler.getRootAccount());
                      AoservDaemonConnector daemonConnnector = DaemonHandler.getDaemonConnector(database, xenPhysicalServer);
                      // Get the DRBD states
                      List<Server.DrbdReport> drbdReports = Server.parseDrbdReport(daemonConnnector.getDrbdReport());
                      Set<Integer> primaryMapping = AoCollections.newHashSet(drbdReports.size());
                      Set<Integer> secondaryMapping = AoCollections.newHashSet(drbdReports.size());
                      for (Server.DrbdReport drbdReport : drbdReports) {
                        // Look for primary mappings
                        if (
                            drbdReport.getLocalRole() == Server.DrbdReport.Role.Primary
                                && (
                                drbdReport.getRemoteRole() == Server.DrbdReport.Role.Unconfigured
                                    || drbdReport.getRemoteRole() == Server.DrbdReport.Role.Secondary
                                    || drbdReport.getRemoteRole() == Server.DrbdReport.Role.Unknown
                              )
                        ) {
                          primaryMapping.add(
                              NetHostHandler.getHostForPackageAndName(
                                  database,
                                  rootPackagePkey,
                                  drbdReport.getResourceHostname()
                              )
                          );
                        }
                        // Look for secondary mappings
                        if (
                            drbdReport.getLocalRole() == Server.DrbdReport.Role.Secondary
                                && (
                                drbdReport.getRemoteRole() == Server.DrbdReport.Role.Unconfigured
                                    || drbdReport.getRemoteRole() == Server.DrbdReport.Role.Primary
                                    || drbdReport.getRemoteRole() == Server.DrbdReport.Role.Unknown
                              )
                        ) {
                          secondaryMapping.add(
                              NetHostHandler.getHostForPackageAndName(
                                  database,
                                  rootPackagePkey,
                                  drbdReport.getResourceHostname()
                              )
                          );
                        }
                      }
                      // Get the auto-start list
                      Set<String> autoStartList = daemonConnnector.getXenAutoStartLinks();
                      Set<Integer> autoMapping = AoCollections.newHashSet(autoStartList.size());
                      for (String serverName : autoStartList) {
                        autoMapping.add(
                            NetHostHandler.getHostForPackageAndName(
                                database,
                                rootPackagePkey,
                                serverName
                            )
                        );
                      }
                      return new Tuple3<>(
                          primaryMapping,
                          secondaryMapping,
                          autoMapping
                      );
                    } catch (ThreadDeath td) {
                      throw td;
                    } catch (Throwable t) {
                      if (c == (attempts - 1) && Thread.currentThread().isInterrupted()) {
                        throw t;
                      }
                      logger.log(Level.SEVERE, null, t);
                      try {
                        Thread.sleep(2000);
                      } catch (InterruptedException err) {
                        logger.log(Level.WARNING, null, err);
                        // Restore the interrupted status
                        Thread.currentThread().interrupt();
                      }
                    }
                  }
                  throw new AssertionError("Exception should have been thrown when c == " + (attempts - 1) + " or thread interrupted");
                })
            );
          }
          Map<Integer, Set<Integer>> newPrimaryMappings = AoCollections.newHashMap(futures.size());
          Map<Integer, Set<Integer>> newSecondaryMappings = AoCollections.newHashMap(futures.size());
          Map<Integer, Set<Integer>> newAutoMappings = AoCollections.newHashMap(futures.size());
          for (Map.Entry<Integer, Future<Tuple3<Set<Integer>, Set<Integer>, Set<Integer>>>> future : futures.entrySet()) {
            Integer xenPhysicalServer = future.getKey();
            try {
              Tuple3<Set<Integer>, Set<Integer>, Set<Integer>> retVal = future.getValue().get(30, TimeUnit.SECONDS);
              newPrimaryMappings.put(xenPhysicalServer, retVal.getElement1());
              newSecondaryMappings.put(xenPhysicalServer, retVal.getElement2());
              newAutoMappings.put(xenPhysicalServer, retVal.getElement3());
            } catch (ThreadDeath td) {
              throw td;
            } catch (InterruptedException e) {
              logger.log(Level.SEVERE, "xenPhysicalServer=" + xenPhysicalServer, e);
              // Restore the interrupted status
              Thread.currentThread().interrupt();
            } catch (Throwable t) {
              logger.log(Level.SEVERE, "xenPhysicalServer=" + xenPhysicalServer, t);
            }
          }
          setMappings(
              newPrimaryMappings,
              newSecondaryMappings,
              newAutoMappings
          );
        }
      } catch (ThreadDeath td) {
        throw td;
      } catch (Throwable t) {
        logger.log(Level.SEVERE, null, t);
      }
    }
  }

  public static boolean isClusterAdmin(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
    User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    return mu != null && mu.isClusterAdmin();
  }

  public static void checkClusterAdmin(DatabaseConnection conn, RequestSource source, String action) throws IOException, SQLException {
    if (!isClusterAdmin(conn, source)) {
      throw new SQLException("Cluster administration not allowed, '" + action + "'");
    }
  }
}
