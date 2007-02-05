package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import java.io.*;

/**
 * @author  AO Industries, Inc.
 */
public final class BackupDatabase extends Database {

    /**
     * Only one database accessor is made.
     */
    private static BackupDatabase backupDatabase;

    /**
     * Make no instances.
     */
    private BackupDatabase() throws IOException {
        super(
            MasterConfiguration.getBackupDBDriver(),
            MasterConfiguration.getBackupDBURL(),
            MasterConfiguration.getBackupDBUser(),
            MasterConfiguration.getBackupDBPassword(),
            MasterConfiguration.getBackupDBConnectionPoolSize(),
            MasterConfiguration.getBackupDBMaxConnectionAge(),
            MasterServer.getErrorHandler()
        );
        Profiler.startProfile(Profiler.FAST, BackupDatabase.class, "<init>()", null);
        Profiler.endProfile(Profiler.FAST);
    }
    
    public static BackupDatabase getDatabase() throws IOException {
        Profiler.startProfile(Profiler.FAST, BackupDatabase.class, "getDatabase()", null);
        try {
            synchronized(BackupDatabase.class) {
                if(backupDatabase==null) backupDatabase=new BackupDatabase();
                return backupDatabase;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    public DatabaseConnection createDatabaseConnection() {
        Profiler.startProfile(Profiler.FAST, BackupDatabase.class, "createDatabaseConnection()", null);
        try {
            return new BackupDatabaseConnection(this);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
}
