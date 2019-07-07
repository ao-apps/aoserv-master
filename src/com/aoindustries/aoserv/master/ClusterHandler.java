/*
 * Copyright 2009-2013, 2014, 2015, 2016, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.linux.Server;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.CronJobScheduleMode;
import com.aoindustries.cron.Schedule;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.util.IntList;
import com.aoindustries.util.Tuple3;
import com.aoindustries.util.logging.ProcessTimer;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
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
final public class ClusterHandler implements CronJob {

    private static final Logger logger = LogFactory.getLogger(ClusterHandler.class);

    /**
     * The maximum time for a processing pass.
     */
    private static final long TIMER_MAX_TIME = 60L * 1000L; // Five minutes

    /**
     * The interval in which the administrators will be reminded.
     */
    private static final long TIMER_REMINDER_INTERVAL = 60L * 60L * 1000L; // One hour

    private static boolean started=false;

    public static void start() {
        synchronized(System.out) {
            if(!started) {
                System.out.print("Starting " + ClusterHandler.class.getSimpleName() + ": ");
                CronDaemon.addCronJob(new ClusterHandler(), logger);
                started=true;
                System.out.println("Done");
                // Run immediately to populate mapping on start-up
                MasterServer.executorService.submit(() -> {
					updateMappings();
				});
            }
        }
    }

    private ClusterHandler() {
    }

	/**
	 * Runs every minute
	 */
    private static final Schedule schedule = (int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) -> true;

	@Override
    public Schedule getCronJobSchedule() {
        return schedule;
    }

	@Override
    public CronJobScheduleMode getCronJobScheduleMode() {
        return CronJobScheduleMode.SKIP;
    }

	@Override
    public String getCronJobName() {
        return "ClusterHandler";
    }

	@Override
    public int getCronJobThreadPriority() {
        return Thread.NORM_PRIORITY + 1;
    }

	@Override
    public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
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
    }

    private static final Object mappingsLock = new Object();

	/**
	 * The set of virtual servers that have primary DRBD roles on a per physical
	 * server basis.
	 */
    private static Map<Integer,Set<Integer>> primaryMappings = Collections.emptyMap();

	/**
	 * The set of virtual servers that have secondary DRBD roles on a per physical
	 * server basis.
	 */
    private static Map<Integer,Set<Integer>> secondaryMappings = Collections.emptyMap();

	/**
	 * The set of virtual servers that have Xen auto start links on a per physical
	 * server basis.
	 */
    private static Map<Integer,Set<Integer>> autoMappings = Collections.emptyMap();

	private static void setMappings(
		Map<Integer,Set<Integer>> newPrimaryMappings,
		Map<Integer,Set<Integer>> newSecondaryMappings,
		Map<Integer,Set<Integer>> newAutoMappings
	) {
		synchronized(mappingsLock) {
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
        synchronized(mappingsLock) {
            for(Map.Entry<Integer,Set<Integer>> entry : primaryMappings.entrySet()) {
                if(entry.getValue().contains(virtualServerInt)) {
                    if(physicalServerFound) throw new ClusterException("Virtual server #" + virtualServer + " primary found on more than one physical server");
                    physicalServer = entry.getKey();
                    physicalServerFound = true;
                }
            }
			if(!physicalServerFound) {
				for(Map.Entry<Integer,Set<Integer>> entry : autoMappings.entrySet()) {
					if(entry.getValue().contains(virtualServerInt)) {
						if(physicalServerFound) throw new ClusterException("Virtual server #" + virtualServer + " auto start link found on more than one physical server");
						physicalServer = entry.getKey();
						physicalServerFound = true;
					}
				}
			}
        }
        if(!physicalServerFound) throw new ClusterException("Virtual server #" + virtualServer + " primary not found on any physical server");
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
        synchronized(mappingsLock) {
			// Find the set of all physical servers that have this as secondary
			Set<Integer> physicalServers = new HashSet<>();
            for(Map.Entry<Integer,Set<Integer>> entry : secondaryMappings.entrySet()) {
                if(entry.getValue().contains(virtualServerInt)) {
					physicalServers.add(entry.getKey());
                }
            }
			// None found
			if(physicalServers.isEmpty()) {
		        throw new ClusterException("Virtual server #" + virtualServer + " secondary not found on any physical server");
			}
			// If there is only one secondary, use it if not auto
			else if(physicalServers.size()==1) {
				Integer physicalServer1 = physicalServers.iterator().next();
				Set<Integer> autoMappings1 = autoMappings.get(physicalServer1);
				boolean auto = autoMappings1!=null && autoMappings1.contains(virtualServer);
				if(auto) {
					throw new ClusterException("Virtual server #" + virtualServer + " secondary only found on physical server with auto start link: " + physicalServer1);
				}
				return physicalServer1;
			}
			// If two, choose the one that is not auto-start
			else if(physicalServers.size()==2) {
				Iterator<Integer> iter = physicalServers.iterator();
				Integer physicalServer1 = iter.next();
				Integer physicalServer2 = iter.next();
				// Get the auto mappings for each
				Set<Integer> autoMappings1 = autoMappings.get(physicalServer1);
				Set<Integer> autoMappings2 = autoMappings.get(physicalServer2);
				// Find if has on auto
				boolean auto1 = autoMappings1!=null && autoMappings1.contains(virtualServer);
				boolean auto2 = autoMappings2!=null && autoMappings2.contains(virtualServer);
				// Resolve based on auto mappings
				if(auto1) {
					if(auto2) {
						// auto1 && auto2
						throw new ClusterException("Virtual server #" + virtualServer + " auto start link found on both physical servers: " + physicalServer1 + " and " + physicalServer2);
					} else {
						// auto1 && !auto2
						return physicalServer2;
					}
				} else {
					if(auto2) {
						// !auto1 && auto2
						return physicalServer1;
					} else {
						// !auto1 && !auto2
						throw new ClusterException("Virtual server #" + virtualServer + " auto start link not found on either physical server: " + physicalServer1 + " or " + physicalServer2);
					}
				}
			}
			// Error if more than two
			else {
		        throw new ClusterException("Virtual server #" + virtualServer + " secondary found on more than two physical servers: " + physicalServers);
			}
        }
    }

	private static final Object updateMappingsLock = new Object();
    private static void updateMappings() {
        synchronized(updateMappingsLock) {
            try {
                ProcessTimer timer=new ProcessTimer(
                    logger,
                    ClusterHandler.class.getName(),
                    "runCronJob",
                    "ClusterHandler - Find Virtual Host Mapping",
                    "Finding the current mapping of virtual servers onto physical servers",
                    TIMER_MAX_TIME,
                    TIMER_REMINDER_INTERVAL
                );
                try {
                    MasterServer.executorService.submit(timer);

                    // Query the servers in parallel
                    final MasterDatabase database = MasterDatabase.getDatabase();
                    IntList xenPhysicalServers = ServerHandler.getEnabledXenPhysicalServers(database);
                    Map<Integer,Future<Tuple3<Set<Integer>,Set<Integer>,Set<Integer>>>> futures = new HashMap<>(xenPhysicalServers.size()*4/3+1);
                    for(final Integer xenPhysicalServer : xenPhysicalServers) {
                        futures.put(
                            xenPhysicalServer,
                            MasterServer.executorService.submit(() -> {
								// Try up to ten times
								for(int c=0;c<10;c++) {
									try {
										final int rootPackagePkey = PackageHandler.getPKeyForPackage(database, BusinessHandler.getRootBusiness());
										AOServDaemonConnector daemonConnnector = DaemonHandler.getDaemonConnector(database, xenPhysicalServer);
										// database.releaseConnection();
										// Get the DRBD states
										List<Server.DrbdReport> drbdReports = Server.parseDrbdReport(daemonConnnector.getDrbdReport());
										Set<Integer> primaryMapping = new HashSet<>(drbdReports.size()*4/3+1);
										Set<Integer> secondaryMapping = new HashSet<>(drbdReports.size()*4/3+1);
										for(Server.DrbdReport drbdReport : drbdReports) {
											// Look for primary mappings
											if(
												drbdReport.getLocalRole()==Server.DrbdReport.Role.Primary
												&& (
													drbdReport.getRemoteRole()==Server.DrbdReport.Role.Unconfigured
													|| drbdReport.getRemoteRole()==Server.DrbdReport.Role.Secondary
													|| drbdReport.getRemoteRole()==Server.DrbdReport.Role.Unknown
												)
											) {
												primaryMapping.add(
													ServerHandler.getServerForPackageAndName(
														database,
														rootPackagePkey,
														drbdReport.getResourceHostname()
													)
												);
											}
											// Look for secondary mappings
											if(
												drbdReport.getLocalRole()==Server.DrbdReport.Role.Secondary
												&& (
													drbdReport.getRemoteRole()==Server.DrbdReport.Role.Unconfigured
													|| drbdReport.getRemoteRole()==Server.DrbdReport.Role.Primary
													|| drbdReport.getRemoteRole()==Server.DrbdReport.Role.Unknown
												)
											) {
												secondaryMapping.add(
													ServerHandler.getServerForPackageAndName(
														database,
														rootPackagePkey,
														drbdReport.getResourceHostname()
													)
												);
											}
										}
										// Get the auto-start list
										Set<String> autoStartList = daemonConnnector.getXenAutoStartLinks();
										Set<Integer> autoMapping = new HashSet<>(autoStartList.size()*4/3+1);
										for(String serverName : autoStartList) {
											autoMapping.add(
												ServerHandler.getServerForPackageAndName(
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
									} catch(Exception exception) {
										if(c==9) throw exception;
										LogFactory.getLogger(ClusterHandler.class).log(Level.SEVERE, null, exception);
										try {
											Thread.sleep(2000);
										} catch(InterruptedException err) {
											LogFactory.getLogger(ClusterHandler.class).log(Level.WARNING, null, err);
										}
									}
								}
								throw new AssertionError("Exception should have been thrown when c==9");
							})
                        );
                    }
                    Map<Integer,Set<Integer>> newPrimaryMappings = new HashMap<>(futures.size()*4/3+1);
                    Map<Integer,Set<Integer>> newSecondaryMappings = new HashMap<>(futures.size()*4/3+1);
                    Map<Integer,Set<Integer>> newAutoMappings = new HashMap<>(futures.size()*4/3+1);
                    for(Map.Entry<Integer,Future<Tuple3<Set<Integer>,Set<Integer>,Set<Integer>>>> future : futures.entrySet()) {
                        Integer xenPhysicalServer = future.getKey();
                        try {
							Tuple3<Set<Integer>,Set<Integer>,Set<Integer>> retVal = future.getValue().get(30, TimeUnit.SECONDS);
                            newPrimaryMappings.put(xenPhysicalServer, retVal.getElement1());
                            newSecondaryMappings.put(xenPhysicalServer, retVal.getElement2());
                            newAutoMappings.put(xenPhysicalServer, retVal.getElement3());
                        } catch(ThreadDeath TD) {
                            throw TD;
                        } catch(Throwable T) {
                            logger.log(Level.SEVERE, "xenPhysicalServer="+xenPhysicalServer, T);
                        }
                    }
					setMappings(
						newPrimaryMappings,
						newSecondaryMappings,
						newAutoMappings
					);
                } finally {
                    timer.finished();
                }
            } catch(ThreadDeath TD) {
                throw TD;
            } catch(Throwable T) {
                logger.log(Level.SEVERE, null, T);
            }
        }
    }

	public static boolean isClusterAdmin(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
        User mu=MasterServer.getUser(conn, source.getUsername());
        return mu!=null && mu.isClusterAdmin();
    }

	public static void checkClusterAdmin(DatabaseConnection conn, RequestSource source, String action) throws IOException, SQLException {
        if(!isClusterAdmin(conn, source)) throw new SQLException("Cluster administration not allowed, '"+action+"'");
    }
}
