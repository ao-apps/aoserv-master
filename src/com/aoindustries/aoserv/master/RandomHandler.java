package com.aoindustries.aoserv.master;

/*
 * Copyright 2004-2008 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.io.*;
import com.aoindustries.profiler.*;
import java.io.*;
import java.sql.*;

/**
 * The <code>RandomHandler</code> stores obtains a pool of random data from servers that have excess and provides
 * this data to those servers that need more.
 *
 * @author  AO Industries, Inc.
 */
public final class RandomHandler {

    private static FifoFile fifoFile;
    
    public static FifoFile getFifoFile() throws IOException {
        Profiler.startProfile(Profiler.FAST, RandomHandler.class, "getFifoFile()", null);
        try {
            synchronized(RandomHandler.class) {
                if(fifoFile==null) fifoFile=new FifoFile(MasterConfiguration.getEntropyPoolFilePath(), AOServConnector.MASTER_ENTROPY_POOL_SIZE);
                return fifoFile;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    private static void checkAccessEntropy(MasterDatabaseConnection conn, RequestSource source, String action) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, RandomHandler.class, "checkAccessEntropy(MasterDatabaseConnection,RequestSource,String)", null);
        try {
            boolean isAllowed=false;

            String mustring=source.getUsername();
            MasterUser mu = MasterServer.getMasterUser(conn, mustring);
            if (mu!=null) {
                com.aoindustries.aoserv.client.MasterServer[] masterServers=MasterServer.getMasterServers(conn, mustring);
                if(masterServers.length==0) isAllowed=true;
                else {
                    for(int c=0;c<masterServers.length;c++) {
                        if(ServerHandler.isAOServer(conn, masterServers[c].getServerPKey())) {
                            isAllowed=true;
                            break;
                        }
                    }
                }
            }
            if(!isAllowed) {
                String message=
                    "business_administrator.username="
                    +mustring
                    +" is not allowed to access the master entropy pool: action='"
                    +action
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void addMasterEntropy(
        MasterDatabaseConnection conn,
        RequestSource source,
        byte[] entropy
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, RandomHandler.class, "addMasterEntropy(MasterDatabaseConnection,RequestSource,byte[])", null);
        try {
            checkAccessEntropy(conn, source, "addMasterEntropy");
            
            FifoFile file=getFifoFile();
            synchronized(file) {
                FifoFileOutputStream fileOut=file.getOutputStream();
                long available=fileOut.available();
                int addCount=entropy.length;
                if(available<addCount) addCount=(int)available;
                fileOut.write(entropy, 0, addCount);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static byte[] getMasterEntropy(
        MasterDatabaseConnection conn,
        RequestSource source,
        int numBytes
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, RandomHandler.class, "getMasterEntropy(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            checkAccessEntropy(conn, source, "getMasterEntropy");
            
            FifoFile file=getFifoFile();
            synchronized(file) {
                FifoFileInputStream fileIn=file.getInputStream();
                long available=fileIn.available();
                if(available<numBytes) numBytes=(int)available;
                byte[] buff=new byte[numBytes];
                int pos=0;
                while(pos<numBytes) {
                    int ret=fileIn.read(buff, pos, numBytes-pos);
                    if(ret==-1) throw new EOFException("Unexpected EOF");
                    pos+=ret;
                }
                return buff;
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static long getMasterEntropyNeeded(
        MasterDatabaseConnection conn,
        RequestSource source
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, RandomHandler.class, "getMasterEntropyNeeded(MasterDatabaseConnection,RequestSource)", null);
        try {
            checkAccessEntropy(conn, source, "getMasterEntropyNeeded");
            
            FifoFile file=getFifoFile();
            synchronized(file) {
                return file.getMaximumFifoLength()-file.getLength();
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}