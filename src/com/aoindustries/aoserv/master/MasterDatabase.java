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
public final class MasterDatabase extends Database {

    /**
     * Only one database accessor is made.
     */
    private static MasterDatabase masterDatabase;

    /**
     * Make no instances.
     */
    private MasterDatabase() throws IOException {
        super(
            MasterConfiguration.getDBDriver(),
            MasterConfiguration.getDBURL(),
            MasterConfiguration.getDBUser(),
            MasterConfiguration.getDBPassword(),
            MasterConfiguration.getDBConnectionPoolSize(),
            MasterConfiguration.getDBMaxConnectionAge(),
            MasterServer.getErrorHandler()
        );
        Profiler.startProfile(Profiler.FAST, MasterDatabase.class, "<init>()", null);
        Profiler.endProfile(Profiler.FAST);
    }
    
    public static MasterDatabase getDatabase() throws IOException {
        Profiler.startProfile(Profiler.FAST, MasterDatabase.class, "getDatabase()", null);
        try {
            synchronized(MasterDatabase.class) {
                if(masterDatabase==null) masterDatabase=new MasterDatabase();
                return masterDatabase;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public DatabaseConnection createDatabaseConnection() {
        Profiler.startProfile(Profiler.FAST, MasterDatabase.class, "createDatabaseConnection()", null);
        try {
            return new MasterDatabaseConnection(this);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
}
