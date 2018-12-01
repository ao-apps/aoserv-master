/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.master.Process;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.net.InetAddress;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final public class MasterProcessManager {

    /**
     * Make no instances.
     */
    private MasterProcessManager() {
    }

    private static final List<Process> processes=new ArrayList<>();

    private static long nextPID=1;

    public static Process createProcess(
        InetAddress host,
        String protocol,
        boolean is_secure
    ) {
        synchronized(MasterProcessManager.class) {
            long time=System.currentTimeMillis();
            Process process=new Process(
                nextPID++,
                host,
                protocol,
                is_secure,
                time
            );
            processes.add(process);
            return process;
        }
    }

    public static void removeProcess(Process process) {
        synchronized(MasterProcessManager.class) {
            int size=processes.size();
            for(int c=0;c<size;c++) {
                Process mp=processes.get(c);
                if(mp.getProcessID()==process.getProcessID()) {
                    processes.remove(c);
                    return;
                }
            }
            throw new IllegalStateException("Unable to find process #"+process.getProcessID()+" in the process list");
        }
    }
    
    public static void writeProcesses(
        DatabaseConnection conn,
        CompressedDataOutputStream out,
        boolean provideProgress,
        RequestSource source,
        User masterUser,
        UserHost[] masterServers
    ) throws IOException, SQLException {
        List<Process> processesCopy=new ArrayList<>(processes.size());
        synchronized(MasterProcessManager.class) {
            processesCopy.addAll(processes);
        }
        List<Process> objs=new ArrayList<>();
        Iterator<Process> I=processesCopy.iterator();
        while(I.hasNext()) {
            Process process=I.next();
            if(masterUser!=null && masterServers.length==0) {
                // Stupor-user
                objs.add(process);
            } else {
                UserId effectiveUser = process.getEffectiveUser();
                if(
                    effectiveUser!=null
                    && UsernameHandler.canAccessUsername(conn, source, effectiveUser)
                ) objs.add(process);
            }
        }
        MasterServer.writeObjectsSynced(source, out, provideProgress, objs);
    }
}