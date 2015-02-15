/*
 * Copyright 2001-2013, 2015 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.dbc.Database;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * @author  AO Industries, Inc.
 */
public final class MasterDatabase extends Database {

    /**
     * This logger doesn't use ticket logger because it might create a loop
     * by logging database errors to the database.
     */
    private static final Logger logger = Logger.getLogger(MasterDatabase.class.getName());

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
            logger
        );
    }
    
    public static MasterDatabase getDatabase() throws IOException {
        synchronized(MasterDatabase.class) {
            if(masterDatabase==null) masterDatabase=new MasterDatabase();
            return masterDatabase;
        }
    }
}
