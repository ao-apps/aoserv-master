/*
 * Copyright 2009-2013 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.AOServer;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.CronJobScheduleMode;
import com.aoindustries.cron.Schedule;
import com.aoindustries.util.logging.ProcessTimer;
import com.aoindustries.util.IntList;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The <code>ClusterHandler</code> maintains a mapping of virtual servers
 * to physical servers.  It updates its mapping every five minutes.
 *
 * @author  AO Industries, Inc.
 */
final public class ClusterHandler implements CronJob {

    private static final Logger logger = LogFactory.getLogger(ClusterHandler.class);

    /**
     * The maximum time for a processing pass.
     */
    private static final long TIMER_MAX_TIME=5L*60*1000; // Five minutes

    /**
     * The interval in which the administrators will be reminded.
     */
    private static final long TIMER_REMINDER_INTERVAL=1L*60*60*1000; // One hour

    private static boolean started=false;

    public static void start() {
        synchronized(System.out) {
            if(!started) {
                System.out.print("Starting ClusterHandler: ");
                CronDaemon.addCronJob(new ClusterHandler(), logger);
                started=true;
                System.out.println("Done");
                // Run immediately to populate mapping on start-up
                MasterServer.executorService.submit(
                    new Runnable() {
						@Override
                        public void run() {
                            updateMappings();
                        }
                    }
                );
            }
        }
    }

    private ClusterHandler() {
    }

    private static final Schedule schedule = new Schedule() {
        /**
         * Runs every five minutes on 2, 7, ...
         */
		@Override
        public boolean isCronJobScheduled(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
            return (minute%5)==2;
        }
    };

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
        return Thread.NORM_PRIORITY+1;
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
    private static Map<Integer,Set<Integer>> primaryMappings = Collections.emptyMap();

    /**
     * Gets the pkey of the physical server that is currently the primary for the virtual server.
     *
     */
    public static int getPrimaryPhysicalServer(int virtualServer) throws ClusterException {
        Integer virtualServerInt = Integer.valueOf(virtualServer);
        int physicalServer = -1;
        boolean physicalServerFound = false;
        synchronized(mappingsLock) {
            for(Map.Entry<Integer,Set<Integer>> entry : primaryMappings.entrySet()) {
                if(entry.getValue().contains(virtualServerInt)) {
                    if(physicalServerFound) throw new ClusterException("Virtual server primary found on more than one physical server");
                    physicalServer = entry.getKey();
                    physicalServerFound = true;
                }
            }
        }
        if(!physicalServerFound) throw new ClusterException("Virtual server primary not found on any physical server");
        return physicalServer;
    }

    private static final Object updateMappingsLock = new Object();
    private static void updateMappings() {
        synchronized(updateMappingsLock) {
            try {
                ProcessTimer timer=new ProcessTimer(
                    logger,
                    MasterServer.getRandom(),
                    ClusterHandler.class.getName(),
                    "runCronJob",
                    "ClusterHandler - Find Virtual Server Mapping",
                    "Finding the current mapping of virtual servers onto physical servers",
                    TIMER_MAX_TIME,
                    TIMER_REMINDER_INTERVAL
                );
                try {
                    MasterServer.executorService.submit(timer);

                    // Query the servers in parallel
                    final MasterDatabase database = MasterDatabase.getDatabase();
                    IntList xenPhysicalServers = ServerHandler.getEnabledXenPhysicalServers(database);
                    Map<Integer,Future<Set<Integer>>> futures = new HashMap<>(xenPhysicalServers.size()*4/3+1);
                    for(final Integer xenPhysicalServer : xenPhysicalServers) {
                        futures.put(
                            xenPhysicalServer,
                            MasterServer.executorService.submit(
                                new Callable<Set<Integer>>() {
									@Override
                                    public Set<Integer> call() throws Exception {
                                        // Try up to ten times
                                        for(int c=0;c<10;c++) {
                                            try {
                                                List<AOServer.DrbdReport> drbdReports = AOServer.parseDrbdReport(DaemonHandler.getDaemonConnector(database, xenPhysicalServer).getDrbdReport());
                                                Set<Integer> primaryMapping = new HashSet<>(drbdReports.size()*4/3+1);
                                                for(AOServer.DrbdReport drbdReport : drbdReports) {
                                                    if(
                                                        drbdReport.getLocalRole()==AOServer.DrbdReport.Role.Primary
                                                        && (
                                                            drbdReport.getRemoteRole()==AOServer.DrbdReport.Role.Unconfigured
                                                            || drbdReport.getRemoteRole()==AOServer.DrbdReport.Role.Secondary
                                                            || drbdReport.getRemoteRole()==AOServer.DrbdReport.Role.Unknown
                                                        )
                                                    ) {
                                                        primaryMapping.add(
                                                            ServerHandler.getServerForPackageAndName(
                                                                database,
                                                                PackageHandler.getPKeyForPackage(database, BusinessHandler.getRootBusiness().toString()),
                                                                drbdReport.getResourceHostname()
                                                            )
                                                        );
                                                    }
                                                }
                                                return primaryMapping;
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
                                    }
                                }
                            )
                        );
                    }
                    Map<Integer,Set<Integer>> newPrimaryMappings = new HashMap<>(futures.size()*4/3+1);
                    for(Map.Entry<Integer,Future<Set<Integer>>> future : futures.entrySet()) {
                        Integer xenPhysicalServer = future.getKey();
                        try {
                            newPrimaryMappings.put(xenPhysicalServer, future.getValue().get(60, TimeUnit.SECONDS));
                        } catch(ThreadDeath TD) {
                            throw TD;
                        } catch(Throwable T) {
                            logger.log(Level.SEVERE, "xenPhysicalServer="+xenPhysicalServer, T);
                        }
                    }
                    synchronized(mappingsLock) {
                        primaryMappings = newPrimaryMappings;
                    }
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
}