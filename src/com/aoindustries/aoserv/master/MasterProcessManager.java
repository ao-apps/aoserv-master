package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.io.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * @author  AO Industries, Inc.
 */
final public class MasterProcessManager {

    private static final List<MasterProcess> processes=new ArrayList<MasterProcess>();

    private static long nextPID=1;

    public static MasterProcess createProcess(
        String host,
        String protocol,
        boolean is_secure
    ) {
        synchronized(MasterProcessManager.class) {
            long time=System.currentTimeMillis();
            MasterProcess process=new MasterProcess(
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

    public static void removeProcess(MasterProcess process) {
        synchronized(MasterProcessManager.class) {
            int size=processes.size();
            for(int c=0;c<size;c++) {
                MasterProcess mp=processes.get(c);
                if(mp.getProcessID()==process.getProcessID()) {
                    processes.remove(c);
                    return;
                }
            }
            throw new IllegalStateException("Unable to find process #"+process.getProcessID()+" in the process list");
        }
    }
    
    public static void writeProcesses(
        MasterDatabaseConnection conn,
        CompressedDataOutputStream out,
        boolean provideProgress,
        RequestSource source,
        MasterUser masterUser,
        com.aoindustries.aoserv.client.MasterServer[] masterServers
    ) throws IOException, SQLException {
        List<MasterProcess> processesCopy=new ArrayList<MasterProcess>(processes.size());
        synchronized(MasterProcessManager.class) {
            processesCopy.addAll(processes);
        }
        List<MasterProcess> objs=new ArrayList<MasterProcess>();
        Iterator I=processesCopy.iterator();
        while(I.hasNext()) {
            MasterProcess process=(MasterProcess)I.next();
            if(masterUser!=null && masterServers.length==0) {
                // Stupor-user
                objs.add(process);
            } else {
                String effectiveUser = process.getEffectiveUser();
                if(
                    effectiveUser!=null
                    && UsernameHandler.canAccessUsername(conn, source, effectiveUser)
                ) objs.add(process);
            }
        }
        MasterServer.writeObjectsSynced(source, out, provideProgress, objs);
    }
}