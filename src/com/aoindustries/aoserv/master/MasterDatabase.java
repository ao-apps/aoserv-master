package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
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
    }
    
    public static MasterDatabase getDatabase() throws IOException {
        synchronized(MasterDatabase.class) {
            if(masterDatabase==null) masterDatabase=new MasterDatabase();
            return masterDatabase;
        }
    }

    @Override
    public DatabaseConnection createDatabaseConnection() {
        return new MasterDatabaseConnection(this);
    }
}
